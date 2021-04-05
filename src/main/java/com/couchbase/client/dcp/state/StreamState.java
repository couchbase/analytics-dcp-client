/*
 * Copyright (c) 2016-2021 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

import static com.couchbase.client.dcp.util.CollectionsUtil.displayCid;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.util.ShortSortedBitSet;

import it.unimi.dsi.fastutil.shorts.ShortSet;
import it.unimi.dsi.fastutil.shorts.ShortSets;

/**
 * Holds the state information for the current session (all partitions involved).
 */
public class StreamState {
    private static final Logger LOGGER = LogManager.getLogger();

    private final SessionState sessionState;

    private final int streamId;

    private final int cid;

    /**
     * Contains states for each individual partition.
     */
    private final StreamPartitionState[] partitionStates;

    private volatile CountDownLatch currentSeqLatch = new CountDownLatch(0);

    private volatile Throwable seqsRequestFailure;

    private long pendingItemCount = 0;

    private long itemCount = -1;

    private final Semaphore collectionItemSemaphore = new Semaphore(0);

    private final ShortSet pendingSeqnos = ShortSets.synchronize(new ShortSortedBitSet());

    /**
     * Initializes a StreamState
     */
    public StreamState(int streamId, int cid, SessionState sessionState, short[] vbuckets) {
        this.streamId = streamId;
        this.cid = cid;
        this.sessionState = sessionState;
        this.partitionStates = new StreamPartitionState[sessionState.getNumOfPartitions()];
        if (vbuckets.length > 0) {
            for (short vbid : vbuckets) {
                partitionStates[vbid] = new StreamPartitionState(vbid);
            }
        } else {
            for (short vbid = 0; vbid < partitionStates.length; vbid++) {
                partitionStates[vbid] = new StreamPartitionState(vbid);
            }
        }
    }

    public SessionState session() {
        return sessionState;
    }

    /**
     * Accessor into the partition state, only use this if really needed.
     * <p>
     * If you want to avoid going out of bounds, use the simpler iterator way on {@link #partitionStream()}.
     *
     * @param partition the index of the partition.
     * @return the partition state for the given partition id.
     */
    public StreamPartitionState get(final int partition) {
        return partitionStates[partition];
    }

    /**
     * Accessor into the partition state
     *
     * @param partition the index of the partition.
     * @return the partition state for the given partition id, otherwise {@link Optional#empty()}
     */
    public Optional<StreamPartitionState> getOptional(final int partition) {
        return partition < partitionStates.length ? Optional.ofNullable(partitionStates[partition]) : Optional.empty();
    }

    /**
     * Accessor to set/override the current partition state, only use this if really needed.
     *
     * @param partition      the index of the partition.
     * @param partitionState the partition state to override.
     */
    public void set(int partition, StreamPartitionState partitionState) {
        partitionStates[partition] = partitionState;
    }

    /**
     * Provides a stream over all partitions
     */
    public Stream<StreamPartitionState> partitionStream() {
        return Stream.of(partitionStates).filter(Objects::nonNull);
    }

    public int getNumOfPartitions() {
        return partitionStates.length;
    }

    @Override
    public String toString() {
        return "StreamState{" + "sid=" + streamId + ", itemCount=" + itemCount + ", partitionStates="
                + Arrays.toString(partitionStates) + '}';
    }

    public int streamId() {
        return streamId;
    }

    public void resetSeqNoRequest(int length) {
        currentSeqLatch = new CountDownLatch(length);
        seqsRequestFailure = null;
        pendingSeqnos.clear();
        partitionStream().mapToInt(StreamPartitionState::vbid).forEach(vbid -> pendingSeqnos.add((short) vbid));
    }

    public void waitTillCurrentSeqUpdated(long timeout) throws Throwable {
        Span span = Span.start(timeout, TimeUnit.MILLISECONDS);
        LOGGER.debug("Waiting until current seq updated for all vbuckets");
        if (!currentSeqLatch.await(span.getSpanNanos(), TimeUnit.NANOSECONDS)) {
            LOGGER.warn("{} elapsed before obtaining current seqnos ({} remaining {})", span,
                    currentSeqLatch.getCount(), pendingSeqnos);
            throw new TimeoutException("waitTillCurrentSeqUpdated");
        }
        if (seqsRequestFailure != null) {
            throw seqsRequestFailure;
        }
    }

    public void interruptPendingSeqnoRequests() {
        if (!pendingSeqnos.isEmpty()) {
            seqsRequestFailed(new InterruptedException());
        }
    }

    public short[] getPendingSeqnos() {
        return pendingSeqnos.toShortArray();
    }

    public void seqsRequestFailed(Throwable t) {
        if (pendingSeqnos.isEmpty()) {
            LOGGER.debug("ignoring unexpected seqno failure", t);
            return;
        }
        seqsRequestFailure = t;
        // drain countdown latch
        for (long i = currentSeqLatch.getCount(); i > 0; i--) {
            currentSeqLatch.countDown();
        }
        pendingSeqnos.clear();
    }

    public void handleSeqnoResponse(short vbid, long seqno) {
        if (pendingSeqnos.isEmpty()) {
            LOGGER.debug("ignoring unexpected seqno update for vbid {}", vbid);
            return;
        }
        final StreamPartitionState ps = get(vbid);
        if (pendingSeqnos.remove(vbid) && ps != null) {
            ps.setCurrentVBucketSeqnoInMaster(seqno);
            currentSeqLatch.countDown();
        }
    }

    public int collectionId() {
        return cid;
    }

    public long itemCount() {
        return itemCount;
    }

    public synchronized void setCollectionItemCount(int cid, long itemCount) {
        if (this.cid != cid) {
            throw new IllegalStateException("received item count for wrong collection: expected " + displayCid(this.cid)
                    + " got " + displayCid(cid));
        }
        if (!collectionItemSemaphore.tryAcquire()) {
            LOGGER.warn("received unexpected collection item count!");
        } else {
            pendingItemCount += itemCount;
            if (collectionItemSemaphore.availablePermits() == 0) {
                LOGGER.debug("setting item count to {} (was {}) for sid {} (cid {})", pendingItemCount, this.itemCount,
                        streamId, displayCid(this.cid));
                this.itemCount = pendingItemCount;
                pendingItemCount = 0;
            }
        }
    }

    public synchronized Semaphore initCollectionItemRequest() {
        if (collectionItemSemaphore.drainPermits() != 0) {
            LOGGER.warn("making new collection item request before previous finished");
            pendingItemCount = 0;
        }
        return collectionItemSemaphore;
    }
}

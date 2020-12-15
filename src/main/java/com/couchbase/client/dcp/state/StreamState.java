/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Holds the state information for the current session (all partitions involved).
 */
public class StreamState {
    private static final Logger LOGGER = LogManager.getLogger();
    /**
     * The maximum number of partitions that can be stored.
     */
    private static final int MAX_PARTITIONS = 1024;

    private final SessionState sessionState;

    private final int streamId;

    private final int cid;

    /**
     * Contains states for each individual partition.
     */
    private final List<StreamPartitionState> partitionStates;

    private volatile CountDownLatch currentSeqLatch = new CountDownLatch(0);

    private volatile Throwable seqsRequestFailure;

    /**
     * Initializes with an empty partition state for 1024 partitions.
     */
    public StreamState(int streamId, int cid, SessionState sessionState) {
        this.streamId = streamId;
        this.cid = cid;
        this.sessionState = sessionState;
        this.partitionStates = new ArrayList<>(MAX_PARTITIONS);
        if (sessionState.getNumOfPartitions() > MAX_PARTITIONS) {
            throw new IllegalArgumentException("Can only hold " + MAX_PARTITIONS + " partitions, "
                    + sessionState.getNumOfPartitions() + "supplied as initializer.");
        }
        for (short vbid = 0; vbid < sessionState.getNumOfPartitions(); vbid++) {
            StreamPartitionState partitionState = new StreamPartitionState(vbid, this);
            partitionStates.add(partitionState);
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
        return partitionStates.get(partition);
    }

    /**
     * Accessor to set/override the current partition state, only use this if really needed.
     *
     * @param partition      the index of the partition.
     * @param partitionState the partition state to override.
     */
    public void set(int partition, StreamPartitionState partitionState) {
        partitionStates.set(partition, partitionState);
    }

    /**
     * Provides a stream over all partitions
     */
    public Stream<StreamPartitionState> partitionStream() {
        return partitionStates.stream().filter(Objects::nonNull);
    }

    public int getNumOfPartitions() {
        return partitionStates.size();
    }

    @Override
    public String toString() {
        return "StreamState{" + "streamId=" + streamId + ", partitionStates=" + partitionStates + '}';
    }

    public int streamId() {
        return streamId;
    }

    public void currentSeqRequest(int length) {
        currentSeqLatch = new CountDownLatch(length);
        seqsRequestFailure = null;
    }

    public void waitTillCurrentSeqUpdated(long timeout) throws Throwable {
        Span span = Span.start(timeout, TimeUnit.MILLISECONDS);
        LOGGER.debug("Waiting until current seq updated for all vbuckets");
        if (!currentSeqLatch.await(span.getSpanNanos(), TimeUnit.NANOSECONDS)) {
            throw new TimeoutException(timeout / 1000.0 + "s passed before obtaining current seqnos ("
                    + currentSeqLatch.getCount() + " remaining)");
        }
        if (seqsRequestFailure != null) {
            throw seqsRequestFailure;
        }
    }

    public void seqsRequestFailed(Throwable t) {
        seqsRequestFailure = t;
        // drain countdown latch
        for (long i = currentSeqLatch.getCount(); i > 0; i--) {
            currentSeqLatch.countDown();
        }
    }

    public void setCurrentVBucketSeqnoInMaster(short vbid, long seqno) {
        get(vbid).setCurrentVBucketSeqnoInMaster(seqno);
        currentSeqLatch.countDown();
    }

    public int collectionId() {
        return cid;
    }
}

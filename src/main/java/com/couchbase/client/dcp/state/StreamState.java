/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.state;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;

/**
 * Holds the state information for the current session (all partitions involved).
 */
public class StreamState {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final long READY_Q_UNINITIALIZED = -1L;
    private final SessionState sessionState;

    private final int streamId;

    private final int[] cids;

    /**
     * Contains states for each individual partition.
     */
    private final StreamPartitionState[] partitionStates;

    private long pendingReadyQItems = 0;
    private long readyQItems = READY_Q_UNINITIALIZED;
    private final Semaphore readyQSemaphore = new Semaphore(0);

    /**
     * Initializes a StreamState
     */
    public StreamState(int streamId, int[] cids, SessionState sessionState, short[] vbuckets) {
        this.streamId = streamId;
        this.cids = cids;
        this.sessionState = sessionState;
        this.partitionStates = new StreamPartitionState[sessionState.getNumOfPartitions()];
        if (vbuckets.length > 0) {
            for (short vbid : vbuckets) {
                partitionStates[vbid] = new StreamPartitionState(this, vbid);
            }
        } else {
            for (short vbid = 0; vbid < partitionStates.length; vbid++) {
                partitionStates[vbid] = new StreamPartitionState(this, vbid);
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
        return "StreamState{" + "sid=" + streamId + ", partitionStates=" + Arrays.toString(partitionStates) + '}';
    }

    public SessionState getSessionState() {
        return sessionState;
    }

    public int streamId() {
        return streamId;
    }

    public int[] cids() {
        return cids;
    }

    public IntSet cidsSet() {
        return new IntAVLTreeSet(cids);
    }

    public boolean isFromZero() {
        return partitionStream().allMatch(ps -> ps.getSnapshotStartSeqno() == 0);
    }

    public long getReadyQItems() {
        return readyQItems;
    }

    public synchronized void recordReadyQItemsResponse(short vbid, long readyQItems) {
        if (!readyQSemaphore.tryAcquire()) {
            LOGGER.warn("received unexpected readyQ items!");
        } else {
            partitionStates[vbid].setReadyQItems(readyQItems);
            pendingReadyQItems += readyQItems;
            if (readyQSemaphore.availablePermits() == 0) {
                LOGGER.debug("[{}] setting sid {} readyQ items to {} (was {})", sessionState.getConfig().name(),
                        streamId, pendingReadyQItems, this.readyQItems);
                this.readyQItems = pendingReadyQItems;
                pendingReadyQItems = 0;
            }
        }
    }

    public synchronized void resetDcpStatsRequest() {
        if (readyQSemaphore.drainPermits() != 0) {
            LOGGER.debug("making new (sid {}) readyQ request before previous finished", streamId);
            pendingReadyQItems = 0;
        }
    }

    public synchronized void registerDcpStatsResponse() {
        readyQSemaphore.release(partitionStates.length);
    }
}

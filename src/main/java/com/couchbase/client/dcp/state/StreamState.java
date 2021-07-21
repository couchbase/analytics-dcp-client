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
import java.util.stream.Stream;

/**
 * Holds the state information for the current session (all partitions involved).
 */
public class StreamState {
    private final SessionState sessionState;

    private final int streamId;

    private final int[] cids;

    /**
     * Contains states for each individual partition.
     */
    private final StreamPartitionState[] partitionStates;

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
}

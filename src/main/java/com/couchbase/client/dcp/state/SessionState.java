/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.message.CollectionsManifest;

import rx.functions.Action1;

/**
 * Holds the state information for the current session (all partitions involved).
 */
public class SessionState {
    private static final Logger LOGGER = LogManager.getLogger();
    /**
     * Special Sequence number defined by DCP which says "no end".
     */
    public static final long NO_END_SEQNO = 0xffffffffffffffffL;

    /**
     * The current version format used on export, respected on import to aid backwards compatibility.
     */
    public static final int CURRENT_VERSION = 1;

    /**
     * The maximum number of partitions that can be stored.
     */
    private static final int MAX_PARTITIONS = 1024;

    /**
     * Contains states for each individual partition.
     */
    private final List<PartitionState> partitionStates;

    private final String uuid;

    private CollectionsManifest collectionsManifest = CollectionsManifest.DEFAULT;

    private Throwable collectionsManifestFailure;

    /**
     * Initializes with an empty partition state for 1024 partitions.
     */
    public SessionState(int numPartitions, String uuid) {
        this.partitionStates = new ArrayList<>(MAX_PARTITIONS);
        this.uuid = uuid;
        if (numPartitions > MAX_PARTITIONS) {
            throw new IllegalArgumentException(
                    "Can only hold " + MAX_PARTITIONS + " partitions, " + numPartitions + "supplied as initializer.");
        }
        for (int i = 0; i < numPartitions; i++) {
            PartitionState partitionState = new PartitionState((short) i);
            partitionStates.add(partitionState);
        }
    }

    /**
     * Accessor into the partition state, only use this if really needed.
     *
     * If you want to avoid going out of bounds, use the simpler iterator way on {@link #foreachPartition(Action1)}.
     *
     * @param partition
     *            the index of the partition.
     * @return the partition state for the given partition id.
     */
    public PartitionState get(final int partition) {
        return partitionStates.get(partition);
    }

    /**
     * Accessor to set/override the current partition state, only use this if really needed.
     *
     * @param partition
     *            the index of the partition.
     * @param partitionState
     *            the partition state to override.
     */
    public void set(int partition, PartitionState partitionState) {
        partitionStates.set(partition, partitionState);
    }

    /**
     * Provides an iterator over all partitions, calling the callback for each one.
     *
     * @param action
     *            the action to be called with the state for every partition.
     */
    public void foreachPartition(final Action1<PartitionState> action) {
        int len = partitionStates.size();
        for (int i = 0; i < len; i++) {
            PartitionState ps = partitionStates.get(i);
            if (ps == null) {
                continue;
            }
            action.call(ps);
        }
    }

    /**
     * Provides a stream over all partitions
     */
    public Stream<PartitionState> partitionStream() {
        return partitionStates.stream().filter(Objects::nonNull);
    }

    /**
     * Export the {@link PartitionState} into the desired format.
     *
     * @param format
     *            the format in which the state should be exposed, always uses the current version.
     * @return the exported format, depending on the type can be converted into a string by the user.
     */
    public byte[] export() {
        return new byte[0];
    }

    public int getNumOfPartitions() {
        return partitionStates.size();
    }

    @Override
    public String toString() {
        return partitionStates.toString();

    }

    public void setConnected(String uuid) {
        if (!uuid.equals(this.uuid)) {
            throw new IllegalStateException("UUID changed from " + this.uuid + " to " + uuid);
        }
        for (PartitionState ps : partitionStates) {
            ps.clientConnected();
        }
    }

    public void setDisconnected() {
        for (PartitionState ps : partitionStates) {
            ps.clientDisconnected();
        }
    }

    public String getUuid() {
        return uuid;
    }

    public synchronized void requestCollectionsManifest(DcpChannel channel) {
        collectionsManifest = null;
        collectionsManifestFailure = null;
        channel.requestCollectionsManifest();
    }

    public synchronized CollectionsManifest waitForCollectionsManifest(long timeout)
            throws InterruptedException, TimeoutException {
        if (collectionsManifest == null) {
            Span span = Span.start(timeout, TimeUnit.MILLISECONDS);
            LOGGER.debug("Waiting until manifest is updated");
            while (collectionsManifest == null && collectionsManifestFailure == null && !span.elapsed()
                    && !partitionStates.get(0).isClientDisconnected()) {
                span.wait(this);
            }
            if (collectionsManifest == null) {
                throw new TimeoutException(timeout / 1000.0 + "s passed before obtaining collections manifest");
            }
        }
        return collectionsManifest;
    }

    public synchronized void onCollectionsManifest(CollectionsManifest collectionsManifest) {
        this.collectionsManifest = collectionsManifest;
        this.collectionsManifestFailure = null;
        for (PartitionState ps : partitionStates) {
            ps.setCollectionsManifest(collectionsManifest);
        }
        notifyAll();
    }

    public synchronized void onCollectionsManifestFailure(Throwable failure) {
        this.collectionsManifest = null;
        this.collectionsManifestFailure = failure;
        notifyAll();
    }

    public synchronized CollectionsManifest getCollectionsManifest() {
        return collectionsManifest;
    }
}

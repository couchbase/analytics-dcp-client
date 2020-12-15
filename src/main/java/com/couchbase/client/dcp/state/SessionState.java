/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

import static it.unimi.dsi.fastutil.objects.ObjectArrays.ensureCapacity;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.message.CollectionsManifest;

/**
 * Holds the state information for the current session (all partitions involved).
 */
public class SessionState {
    private static final Logger LOGGER = LogManager.getLogger();
    /**
     * Special Sequence number defined by DCP which says "no end".
     */
    public static final long NO_END_SEQNO = 0xffffffffffffffffL;

    private final String uuid;

    private final int numPartitions;

    private volatile boolean connected;

    private CollectionsManifest collectionsManifest = CollectionsManifest.DEFAULT;

    private Throwable collectionsManifestFailure;

    private final SessionPartitionState[] sessionPartitionState;

    private volatile StreamState[] streams = new StreamState[0];

    /**
     * Initializes with an empty partition state for 1024 partitions.
     */
    public SessionState(int numPartitions, String uuid) {
        this.numPartitions = numPartitions;
        this.sessionPartitionState = new SessionPartitionState[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            this.sessionPartitionState[i] = new SessionPartitionState((short) i);
        }
        this.uuid = uuid;
        setConnected(uuid);
    }

    /**
     * Provides a (java.util.) Stream over all streams
     */
    public Stream<StreamState> streamStream() {
        return Stream.of(streams).filter(Objects::nonNull);
    }

    public int getNumOfPartitions() {
        return numPartitions;
    }

    @Override
    public String toString() {
        return "SessionState{" + "numPartitions=" + numPartitions + ", uuid='" + uuid + '\'' + ", streams="
                + Stream.of(streams)
                        .map(ss -> "{ streamId : " + ss.streamId() + ", collectionId : 0x"
                                + Integer.toHexString(ss.collectionId()) + " }")
                        .collect(Collectors.joining(", "))
                + '}';
    }

    public void setConnected(String uuid) {
        LOGGER.debug("{} (0x{}): connected", this, Integer.toHexString(System.identityHashCode(this)));
        if (!uuid.equals(this.uuid)) {
            throw new IllegalStateException("UUID changed from " + this.uuid + " to " + uuid);
        }
        connected = true;
    }

    public void setDisconnected() {
        LOGGER.debug("{} (0x{}): disconnected", this, Integer.toHexString(System.identityHashCode(this)));
        connected = false;
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
            LOGGER.debug("Waiting until manifest is received");
            while (collectionsManifest == null && collectionsManifestFailure == null && !span.elapsed() && connected) {
                span.wait(this);
            }
            if (collectionsManifest == null) {
                if (!connected) {
                    throw new InterruptedException("client was disconnected prior to receiving manifest");
                }
                throw new TimeoutException(timeout / 1000.0 + "s passed before obtaining collections manifest");
            }
        }
        return collectionsManifest;
    }

    public synchronized void onCollectionsManifest(CollectionsManifest collectionsManifest) {
        this.collectionsManifest = collectionsManifest;
        this.collectionsManifestFailure = null;
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

    public StreamState streamState(int streamId) {
        return streamId > streams.length ? null : streams[streamId - 1];
    }

    public synchronized StreamState newStream(int streamId, int cid) {
        final StreamState streamState = new StreamState(streamId, cid, this);
        streams = ensureCapacity(streams, streamId);
        streams[streamId - 1] = streamState;
        return streamState;
    }

    public boolean isConnected() {
        return connected;
    }

    public SessionPartitionState get(int vbid) {
        return sessionPartitionState[vbid];
    }

    public void waitTillFailoverUpdated(short vbid, long partitionRequestsTimeout) throws Throwable {
        get(vbid).waitTillFailoverUpdated(this, partitionRequestsTimeout);
    }
}

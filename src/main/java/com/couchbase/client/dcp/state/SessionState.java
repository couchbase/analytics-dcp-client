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

import static it.unimi.dsi.fastutil.objects.ObjectArrays.ensureCapacity;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.dcp.conductor.Conductor;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.message.CollectionsManifest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.CollectionsUtil;
import com.couchbase.client.dcp.util.ShortSortedBitSet;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import it.unimi.dsi.fastutil.shorts.ShortSortedSet;
import it.unimi.dsi.fastutil.shorts.ShortSortedSets;

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

    private final CouchbaseBucketConfig config;

    private volatile boolean connected;

    private CollectionsManifest collectionsManifest = CollectionsManifest.DEFAULT;

    private Throwable collectionsManifestFailure;

    private final AtomicReferenceArray<SessionPartitionState> sessionPartitionState;

    private volatile StreamState[] streams = new StreamState[0];

    private final ShortSortedSet pendingSeqnos;

    private Throwable seqsRequestFailure;

    public SessionState(CouchbaseBucketConfig config) {
        this.config = config;
        this.sessionPartitionState = new AtomicReferenceArray<>(config.numberOfPartitions());
        this.uuid = getUuid(config);
        setConnected(config);
        pendingSeqnos = new ShortSortedBitSet(config.numberOfPartitions());
    }

    private SessionState() {
        this.config = null;
        this.sessionPartitionState = new AtomicReferenceArray<>(0);
        this.uuid = "";
        connected = true;
        pendingSeqnos = ShortSortedSets.EMPTY_SET;
    }

    public static SessionState empty() {
        return new SessionState();
    }

    /**
     * Provides a (java.util.) Stream over all streams
     */
    public Stream<StreamState> streamStream() {
        return Stream.of(streams).filter(Objects::nonNull);
    }

    public int getNumOfPartitions() {
        return sessionPartitionState.length();
    }

    public CouchbaseBucketConfig getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "SessionState{" + "numPartitions=" + sessionPartitionState.length() + ", uuid='" + uuid + '\''
                + ", streams=["
                + Stream.of(streams)
                        .map(ss -> "\"" + ss.streamId() + ":" + CollectionsUtil.displayCid(ss.collectionId()) + '"')
                        .collect(Collectors.joining(", "))
                + "]}";
    }

    public void setConnected(CouchbaseBucketConfig config) {
        String configUuid = getUuid(config);
        LOGGER.debug("{} (0x{}): connected", this, Integer.toHexString(System.identityHashCode(this)));
        if (!configUuid.equals(uuid)) {
            throw new IllegalStateException("UUID changed from " + uuid + " to " + configUuid);
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

    public synchronized StreamState newStream(int streamId, int cid, short... vbuckets) {
        final StreamState streamState = new StreamState(streamId, cid, this, vbuckets);
        streams = ensureCapacity(streams, streamId);
        streams[streamId - 1] = streamState;
        return streamState;
    }

    public boolean isConnected() {
        return connected;
    }

    public SessionPartitionState get(int vbid) {
        SessionPartitionState ps = sessionPartitionState.get(vbid);
        if (ps == null) {
            ps = new SessionPartitionState((short) vbid);
            if (!sessionPartitionState.compareAndSet(vbid, null, ps)) {
                ps = sessionPartitionState.get(vbid);
            }
        }
        return ps;
    }

    public void waitTillFailoverUpdated(short vbid, long partitionRequestsTimeout, TimeUnit timeUnit) throws Throwable {
        get(vbid).waitTillFailoverUpdated(this, partitionRequestsTimeout, timeUnit);
    }

    public void onDataEvent(ByteBuf event) {
        int streamId = MessageUtil.streamId(event);
        StreamPartitionState ps = streamState(streamId).get(MessageUtil.getVbucket(event));
        ps.processDataEvent(event);
    }

    protected static String getUuid(CouchbaseBucketConfig config) {
        return Conductor.getUuid(config.uri());
    }

    public void prepareForSeqnoRequest() {
        seqsRequestFailure = null;
        for (short vbid = 0; vbid < sessionPartitionState.length(); vbid++) {
            pendingSeqnos.add(vbid);
        }
    }

    public void seqnoRequestFailed(Throwable th) {
        seqsRequestFailure = th;
        synchronized (pendingSeqnos) {
            pendingSeqnos.notifyAll();

        }
    }

    public void handleSeqnoResponse(short vbid, long seq) {
        if (pendingSeqnos.remove(vbid)) {
            synchronized (pendingSeqnos) {
                pendingSeqnos.notifyAll();
            }
        }
        get(vbid).setCurrentVBucketSeqnoInMaster(seq);
    }

    public void waitTillSeqnosUpdated(long attemptMillis) throws Throwable {
        Span span = Span.start(attemptMillis, TimeUnit.MILLISECONDS);
        synchronized (pendingSeqnos) {
            while (seqsRequestFailure != null && !pendingSeqnos.isEmpty() && !span.elapsed()) {
                pendingSeqnos.wait(span.remaining(TimeUnit.MILLISECONDS));
            }
        }
        if (seqsRequestFailure != null) {
            throw seqsRequestFailure;
        }
        if (!pendingSeqnos.isEmpty()) {
            LOGGER.warn("{} elapsed before obtaining current seqnos ({} remaining {})", span, pendingSeqnos.size(),
                    pendingSeqnos);
            throw new TimeoutException("waitTillSeqnosUpdated");
        }
    }
}

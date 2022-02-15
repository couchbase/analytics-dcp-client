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

import static com.couchbase.client.dcp.message.MessageUtil.GET_SEQNOS_GLOBAL_COLLECTION_ID;
import static it.unimi.dsi.fastutil.objects.ObjectArrays.ensureCapacity;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.conductor.Conductor;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.message.CollectionsManifest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.CollectionsUtil;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

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

    private final Int2ObjectMap<CollectionState> collectionStates =
            Int2ObjectMaps.synchronize(new Int2ObjectOpenHashMap<>());

    public SessionState(CouchbaseBucketConfig config) {
        this.config = config;
        this.sessionPartitionState = new AtomicReferenceArray<>(config.numberOfPartitions());
        this.uuid = getUuid(config);
        setConnected(config);
    }

    private SessionState() {
        this.config = null;
        this.sessionPartitionState = new AtomicReferenceArray<>(0);
        this.uuid = "";
        connected = true;
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
                        .map(ss -> "\"" + ss.streamId() + ":" + CollectionsUtil.displayCids(ss.cids()) + '"')
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

    public synchronized StreamState newStream(int streamId, int[] cids, short... vbuckets) {
        final StreamState streamState = new StreamState(streamId, cids, this, vbuckets);
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

    public void prepareForSeqnoRequest(int... cids) {
        IntStream.of(cids).forEach(cid -> ensureCollectionState(cid).prepareForRequest());
    }

    public void waitForSeqnos(long attemptMillis, int... cids) throws Throwable {
        final Span span = Span.start(attemptMillis, TimeUnit.MILLISECONDS);
        for (int cid : cids) {
            ensureCollectionState(cid).waitForSeqnos(span);
        }
    }

    public boolean hasSeqnosPending(int... cids) {
        return IntStream.of(cids).anyMatch(cid -> ensureCollectionState(cid).hasPendingSeqnos());
    }

    public void handleSeqnoResponse(int cid, ByteBuf content) {
        ensureCollectionState(cid).handleSeqnoResponse(content);
    }

    public void seqnoRequestFailed(int cid, Throwable th) {
        ensureCollectionState(cid).onFailure(th);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public void seqnoRequestFailIfPending(int cid, Throwable th) {
        CollectionState collectionState = ensureCollectionState(cid);
        synchronized (collectionState) {
            if (collectionState.hasPendingSeqnos()) {
                collectionState.onFailure(th);
            }
        }
    }

    public long getMasterSeqno(int cid, short vbid) {
        return ensureCollectionState(cid).getSeqno(vbid);
    }

    public long getBucketMasterSeqno(short vbid) {
        return getMasterSeqno(GET_SEQNOS_GLOBAL_COLLECTION_ID, vbid);
    }

    public void ensureMaxCurrentVBucketSeqnoInMaster(short vbid, long seqno) {
        ensureCollectionState(GET_SEQNOS_GLOBAL_COLLECTION_ID).ensureMaxSeqno(vbid, seqno);
    }

    public CollectionState getCollectionState(int cid) {
        return ensureCollectionState(cid);
    }

    private CollectionState ensureCollectionState(int cid) {
        return collectionStates.computeIfAbsent(cid, c -> new CollectionState(c, getNumOfPartitions()));
    }

    public void interruptPendingSeqnoRequests() {
        final InterruptedException ex = new InterruptedException();
        synchronized (collectionStates) {
            collectionStates.values().forEach(state -> state.onFailure(ex));
        }
    }

    public void recordItemCountResponse(int cid, long itemCount) {
        ensureCollectionState(cid).recordItemCountResponse(itemCount);
    }

    public void initItemCountRequest(int... cids) {
        IntStream.of(cids).forEach(cid -> ensureCollectionState(cid).resetItemCountRequest());
    }

    public void registerPendingCollectionItemCount(int... cids) {
        IntStream.of(cids).forEach(cid -> ensureCollectionState(cid).registerItemResponse());
    }
}

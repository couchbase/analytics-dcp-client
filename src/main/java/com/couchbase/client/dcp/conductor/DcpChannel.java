/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.conductor;

import static com.couchbase.client.dcp.message.MessageUtil.GET_SEQNOS_GLOBAL_COLLECTION_ID;
import static com.couchbase.client.dcp.util.retry.RetryUtil.shouldRetry;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.core.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpCloseStreamRequest;
import com.couchbase.client.dcp.message.DcpFailoverLogRequest;
import com.couchbase.client.dcp.message.DcpGetCollectionsManifestRequest;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosRequest;
import com.couchbase.client.dcp.message.DcpOpenStreamRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.message.VbucketState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.state.StreamRequest;
import com.couchbase.client.dcp.state.StreamState;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.dcp.transport.netty.Stat;
import com.couchbase.client.dcp.util.CollectionsUtil;

import it.unimi.dsi.fastutil.ints.Int2BooleanMap;
import it.unimi.dsi.fastutil.ints.Int2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

/**
 * Logical representation of a DCP cluster connection.
 *
 * The equals and hashcode are based on the {@link InetAddress}.
 */
public class DcpChannel {
    private static final Logger LOGGER = LogManager.getLogger();
    private volatile State state;
    private final ClientEnvironment env;
    private final String hostname;
    private final InetSocketAddress inetAddress;
    private final boolean[] failoverLogRequests;
    private final IntSet[] openStreams;
    private final SessionState sessionState;
    private final DcpChannelControlMessageHandler controlHandler;
    private volatile Channel channel;
    private final DcpChannelCloseListener closeListener;
    private final long deadConnectionDetectionIntervalNanos;
    private Int2BooleanMap seqnosFetched = new Int2BooleanOpenHashMap();
    private volatile long lastActivityNanoTime = System.currentTimeMillis();
    private boolean channelDroppedReported = false;
    private final boolean collectionCapable;
    private String connectionName;

    public DcpChannel(InetSocketAddress inetAddress, String hostname, final ClientEnvironment env,
            final SessionState sessionState, int numOfPartitions, boolean collectionCapable) {
        setState(State.DISCONNECTED);
        this.inetAddress = inetAddress;
        this.hostname = hostname;
        this.env = env;
        this.sessionState = sessionState;
        this.failoverLogRequests = new boolean[numOfPartitions];
        this.controlHandler = new DcpChannelControlMessageHandler(this);
        this.openStreams = new IntSet[numOfPartitions];
        this.closeListener = new DcpChannelCloseListener(this);
        this.deadConnectionDetectionIntervalNanos =
                TimeUnit.SECONDS.toNanos(env.getDeadConnectionDetectionIntervalSeconds());
        this.collectionCapable = collectionCapable;
    }

    public void connect() throws Throwable {
        connect(env.dcpChannelAttemptTimeout(), env.dcpChannelTotalTimeout(), env.dcpChannelsReconnectDelay());
    }

    public synchronized void connect(long attemptTimeout, long totalTimeout, Delay delay) throws Throwable {
        if (getState() != State.DISCONNECTED) {
            throw new IllegalArgumentException(
                    "Dcp Channel is already connected or is trying to connect. State = " + getState().name());
        }
        setState(State.CONNECTING);
        int attempt = 0;
        Throwable failure = null;
        final long startTime = System.currentTimeMillis();
        boolean infoEnabled = LOGGER.isInfoEnabled();
        while (getState() == State.CONNECTING) {
            attempt++;
            ChannelFuture connectFuture = null;
            try {
                if (infoEnabled) {
                    LOGGER.info("DcpChannel connect attempt #" + attempt + " with socket connect timeout = "
                            + (int) env.dcpChannelAttemptTimeout());
                }
                ByteBufAllocator allocator =
                        env.poolBuffers() ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
                final Bootstrap bootstrap = new Bootstrap().option(ChannelOption.ALLOCATOR, allocator)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) attemptTimeout)
                        .remoteAddress(inetAddress.getHostString(), inetAddress.getPort())
                        .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
                        .handler(new DcpPipeline(this, hostname, inetAddress.getPort(), env, controlHandler))
                        .group(env.eventLoopGroup());
                connectFuture = bootstrap.connect();
                connectFuture.await(attemptTimeout + 100);
                connectFuture.cancel(true);
                if (!connectFuture.isSuccess()) {
                    throw connectFuture.cause();
                }
                LOGGER.debug("Connection established");
                channel = connectFuture.channel();
                setState(State.CONNECTED);
                break;
            } catch (InterruptedException e) {
                LOGGER.warn("Connection was interrupted while attempting to establish DCP connection", e);
                if (connectFuture != null) {
                    final ChannelFuture cf = connectFuture;
                    connectFuture.addListener(f -> {
                        if (f.isSuccess()) {
                            cf.channel().disconnect();
                        }
                    });
                    connectFuture.cancel(true);
                }
                channel = null;
                setState(State.DISCONNECTED);
                throw e;
            } catch (Throwable e) {
                LOGGER.warn("Connection failed", e);
                if (failure == null) {
                    failure = e;
                }
                if (!shouldRetry(e) || System.currentTimeMillis() - startTime > totalTimeout) {
                    LOGGER.warn("Connection FAILED " + attempt + " times");
                    channel = null;
                    setState(State.DISCONNECTED);
                    throw failure; // NOSONAR failure is not nullable
                }
                delay.unit().sleep(delay.calculate(attempt));
            }
        }
        // attempt to restart the dropped streams
        for (short vbid = 0; vbid < openStreams.length; vbid++) {
            if (openStreams[vbid] == null) {
                continue;
            }
            for (int streamId : openStreams[vbid]) {
                // TODO(mblow): consolidate logging
                LOGGER.debug("Opening a stream {} that was dropped for vbucket {}", streamId, vbid);
                final StreamState streamState = sessionState.streamState(streamId);
                StreamPartitionState ps = streamState.get(vbid);
                ps.prepareNextStreamRequest(sessionState, streamState);
                StreamRequest req = ps.getStreamRequest();
                openStream(vbid, req.getVbucketUuid(), req.getStartSeqno(), req.getEndSeqno(),
                        req.getSnapshotStartSeqno(), req.getSnapshotEndSeqno(), req.getManifestUid(), req.getStreamId(),
                        req.getCids());
            }
        }
        for (int i = 0; i < failoverLogRequests.length; i++) {
            if (failoverLogRequests[i]) {
                // TODO(mblow): consolidate logging (use ShortSortedBitSet)
                LOGGER.debug("Re-requesting failover logs for vbucket " + i);
                getFailoverLog((short) i);
            }
        }

        int[] missingSeqnosCids = sessionState.streamStream().map(StreamState::cids).flatMapToInt(IntStream::of)
                .filter(this::isMissingSeqnos).toArray();
        if (missingSeqnosCids.length > 0) {
            requestSeqnos(missingSeqnosCids);
            LOGGER.debug("Re-requested seqnos for cids {}", CollectionsUtil.displayCids(missingSeqnosCids));
        }

        channel.closeFuture().addListener(closeListener);
    }

    public boolean isCollectionCapable() {
        return collectionCapable;
    }

    public State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        this.state = state;
        notifyAll();
    }

    public synchronized void wait(State state) throws InterruptedException {
        while (this.state != state) {
            wait();
        }
    }

    public synchronized void disconnect() {
        LOGGER.info(toString() + " is disconnecting");
        switch (getState()) {
            case CONNECTED:
            case CONNECTING:
                if (channel != null && channel.isOpen()) {
                    setState(State.DISCONNECTING);
                    channel.close();
                } else {
                    setState(State.DISCONNECTED);
                    channel = null;
                    return;
                }
                break;
            default:
                break;
        }
        channel = null;
    }

    public synchronized void openStream(final short vbid, final long vbuuid, final long startSeqno, final long endSeqno,
            final long snapshotStartSeqno, final long snapshotEndSeqno, long manifestUid, int streamId, int[] cids) {
        final StreamState streamState = sessionState.streamState(streamId);
        StreamPartitionState partitionState = streamState.get(vbid);
        if (getState() != State.CONNECTED) {
            StreamEndEvent endEvent = new StreamEndEvent(partitionState, streamState, StreamEndReason.CHANNEL_DROPPED);
            LOGGER.warn("Attempt to open stream on disconnected channel");
            env.eventBus().publish(endEvent);
            return;
        } else if (startSeqno == endSeqno) {
            StreamEndEvent endEvent = new StreamEndEvent(partitionState, streamState, StreamEndReason.OK);
            LOGGER.warn(
                    "Attempt to open stream {} against {} with vbid {} with no requested sequences (start == end) {}",
                    streamId, channel.remoteAddress(), vbid, startSeqno);
            env.eventBus().publish(endEvent);
            return;
        }
        LOGGER.trace(
                "Opening stream {} against {} with vbid {} vbuuid {} startSeqno {} "
                        + "endSeqno {} snapshotStartSeqno {} snapshotEndSeqno {} manifestUid {} cids {}",
                streamId, channel.remoteAddress(), vbid, vbuuid, startSeqno, endSeqno, snapshotStartSeqno,
                snapshotEndSeqno, manifestUid, CollectionsUtil.displayCids(cids));
        partitionState.setState(StreamPartitionState.CONNECTING);
        if (openStreams[vbid] == null) {
            openStreams[vbid] = new IntOpenHashSet();
        }
        openStreams[vbid].add(streamId);
        ByteBuf buffer = Unpooled.buffer();
        DcpOpenStreamRequest.init(buffer, vbid);
        DcpOpenStreamRequest.vbucketStreamId(buffer, vbid, streamId);
        DcpOpenStreamRequest.vbuuid(buffer, vbuuid);
        DcpOpenStreamRequest.startSeqno(buffer, startSeqno);
        DcpOpenStreamRequest.endSeqno(buffer, endSeqno);
        DcpOpenStreamRequest.snapshotStartSeqno(buffer, snapshotStartSeqno);
        DcpOpenStreamRequest.snapshotEndSeqno(buffer, snapshotEndSeqno);

        if (collectionCapable) {
            ObjectMapper om = new ObjectMapper();
            ObjectNode json = om.createObjectNode();
            ArrayNode an = json.putArray("collections");
            IntStream.of(cids).mapToObj(CollectionsUtil::encodeCid).forEach(an::add);
            if (manifestUid != 0) {
                json.put("uid", Long.toUnsignedString(manifestUid, 16));
            }
            if (streamId > 0) {
                json.put("sid", streamId);
            }
            try {
                byte[] value = om.writeValueAsBytes(json);
                DcpOpenStreamRequest.setValue(Unpooled.copiedBuffer(value), buffer);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        ChannelFuture future = channel.writeAndFlush(buffer);
        if (LOGGER.isDebugEnabled()) {
            future.addListener(f -> {
                if (!f.isSuccess()) {
                    LOGGER.debug(
                            "Opening Stream against " + channel.remoteAddress() + " with vbid: " + vbid + " failed",
                            f.cause());
                }
            });
        }
    }

    public synchronized void closeStream(final int streamId, final short vbid) {
        if (getState() != State.CONNECTED) {
            throw new NotConnectedException();
        }
        LOGGER.debug("Closing Stream against {} with vbid: {}", channel.remoteAddress(), vbid);
        sessionState.streamState(streamId).get(vbid).setState(StreamPartitionState.DISCONNECTING);
        openStreams[vbid].remove(streamId);
        ByteBuf buffer = Unpooled.buffer();
        DcpCloseStreamRequest.init(buffer);
        DcpCloseStreamRequest.vbucket(buffer, vbid);
        DcpCloseStreamRequest.opaque(buffer, vbid);
        channel.writeAndFlush(buffer);
    }

    /**
     * Requests seqnos for all vbuckets on this channel for the supplied cids.  The special
     * {@link MessageUtil#GET_SEQNOS_GLOBAL_COLLECTION_ID} is used to retrieve bucket-global seqnos
     */
    public synchronized void requestSeqnos(int... cids) {
        if (cids.length == 0) {
            throw new IllegalArgumentException(
                    "at least one cid (or GET_SEQNOS_GLOBAL_COLLECTION_ID) must be supplied");
        }
        for (int cid : cids) {
            if (!collectionCapable && (cid != 0 && cid != GET_SEQNOS_GLOBAL_COLLECTION_ID)) {
                throw new IllegalArgumentException("illegal cid (" + CollectionsUtil.displayCid(cid)
                        + ") supplied for collections-unaware cluster");
            }
            if (getState() != State.CONNECTED) {
                sessionState.seqnoRequestFailed(cid, new NotConnectedException());
            } else {
                ByteBuf buffer = Unpooled.buffer();
                DcpGetPartitionSeqnosRequest.init(buffer);
                DcpGetPartitionSeqnosRequest.vbucketStateAndCid(buffer, VbucketState.ACTIVE, cid, collectionCapable);
                channel.writeAndFlush(buffer);
            }
        }
    }

    public void requestCollectionItemCounts(int... cids) {
        if (cids.length == 0) {
            throw new IllegalArgumentException("at least one cid must be supplied");
        }
        ByteBuf buffer = Unpooled.buffer();
        Stat.init(buffer);
        for (int cid : cids) {
            Stat.collectionsById(buffer, cid);
            channel.writeAndFlush(buffer);
        }
    }

    public synchronized void getFailoverLog(final short vbid) {
        LOGGER.trace("requesting failover logs for vbucket " + vbid);
        failoverLogRequests[vbid] = true;
        if (getState() != State.CONNECTED) {
            sessionState.get(vbid).failoverLogsRequestFailed(new NotConnectedException());
            return;
        }
        ByteBuf buffer = Unpooled.buffer();
        DcpFailoverLogRequest.init(buffer);
        DcpFailoverLogRequest.opaque(buffer, vbid);
        DcpFailoverLogRequest.vbucket(buffer, vbid);
        channel.writeAndFlush(buffer);
        LOGGER.trace("Asked for failover log on {} for vbid: {}", channel.remoteAddress(), vbid);
    }

    public synchronized void requestCollectionsManifest() {
        LOGGER.debug("requesting collections manifest");
        if (getState() != State.CONNECTED) {
            throw new IllegalStateException("channel is not connected");
        }
        ByteBuf buffer = Unpooled.buffer();
        DcpGetCollectionsManifestRequest.init(buffer);
        channel.writeAndFlush(buffer);
    }

    public IntSet openStreams(short vbid) {
        return openStreams[vbid] != null ? openStreams[vbid] : IntSets.EMPTY_SET;
    }

    // Seriously!?
    @Override
    public boolean equals(Object o) {
        if (o instanceof DcpChannel) {
            return inetAddress.equals(((DcpChannel) o).inetAddress);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return inetAddress.hashCode();
    }

    @Override
    public String toString() {
        return "{\"class\" : \"" + this.getClass().getSimpleName() + "\", \"inetAddress\" : \"" + inetAddress
                + "\", \"bucket\" : \"" + env.bucket() + "\", \"state\" : \"" + state + "\"}";
    }

    public ClientEnvironment getEnv() {
        return env;
    }

    /**
     * The caller of this method is responsible of guarding this set by synchronizing on the channel
     * @return the set of currently open streams
     */
    public IntSet[] openStreams() {
        return openStreams;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public InetSocketAddress getAddress() {
        return inetAddress;
    }

    public ChannelFutureListener getCloseListener() {
        return closeListener;
    }

    public SessionState getSessionState() {
        return sessionState;
    }

    public boolean[] getFailoverLogRequests() {
        return failoverLogRequests;
    }

    public synchronized void seqnosFetched(int cid) {
        seqnosFetched.put(cid, true);
        notifyAll();
    }

    public boolean isSeqnosFetched(int cid) {
        return seqnosFetched.getOrDefault(cid, false);
    }

    public boolean isMissingSeqnos(int cid) {
        return !isSeqnosFetched(cid);
    }

    public synchronized boolean anyStreamIsOpen() {
        return !Stream.of(openStreams).filter(Objects::nonNull).allMatch(IntSet::isEmpty);
    }

    public String getHostname() {
        return hostname;
    }

    public synchronized boolean producerDroppedConnection() {
        if (state != State.CONNECTED || channelDroppedReported || deadConnectionDetectionIntervalNanos == 0) {
            return false;
        }
        long now = System.nanoTime();
        if (now - lastActivityNanoTime > deadConnectionDetectionIntervalNanos) {
            LOGGER.info("Detected dead connection on {}", this);
            return true;
        } else {
            LOGGER.info("Connection {} is not dead", this);
            return false;
        }
    }

    public void newMessageReceived() {
        lastActivityNanoTime = System.nanoTime();
    }

    public void setChannelDroppedReported(boolean b) {
        this.channelDroppedReported = b;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getConnectionName() {
        return connectionName;
    }
}

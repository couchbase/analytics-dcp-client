/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpCloseStreamRequest;
import com.couchbase.client.dcp.message.DcpFailoverLogRequest;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosRequest;
import com.couchbase.client.dcp.message.DcpOpenStreamRequest;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.message.VbucketState;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.deps.io.netty.channel.ChannelOption;

/**
 * Logical representation of a DCP cluster connection.
 *
 * The equals and hashcode are based on the {@link InetAddress}.
 */
public class DcpChannel {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpChannel.class);
    private static final AtomicInteger OPAQUE = new AtomicInteger(0);
    private volatile State state;
    private final ClientEnvironment env;
    private final InetSocketAddress inetAddress;
    private final Map<Integer, Short> vbuckets;
    private final boolean[] failoverLogRequests;
    private final boolean[] openStreams;
    private final SessionState sessionState;
    private final DcpChannelControlMessageHandler controlHandler;
    private volatile Channel channel;
    private final DcpChannelCloseListener closeListener;
    private volatile boolean stateFetched = true;

    public DcpChannel(InetSocketAddress inetAddress, final ClientEnvironment env, final SessionState sessionState,
            int numOfPartitions) {
        setState(State.DISCONNECTED);
        this.inetAddress = inetAddress;
        this.env = env;
        this.sessionState = sessionState;
        this.vbuckets = new ConcurrentHashMap<>();
        this.failoverLogRequests = new boolean[numOfPartitions];
        this.controlHandler = new DcpChannelControlMessageHandler(this);
        this.openStreams = new boolean[numOfPartitions];
        this.closeListener = new DcpChannelCloseListener(this);
    }

    public synchronized void connect() throws Throwable {
        if (getState() != State.DISCONNECTED) {
            throw new IllegalArgumentException(
                    "Dcp Channel is already connected or is trying to connect. State = " + getState().name());
        }
        setState(State.CONNECTING);
        int attempts = 0;
        Throwable failure = null;
        while (attempts < env.dcpChannelsReconnectMaxAttempts() && getState() == State.CONNECTING) {
            attempts++;
            try {
                LOGGER.log(CouchbaseLogLevel.WARN, "DcpChannel connect attempt #" + attempts
                        + " with socket connect timeout = " + (int) env.socketConnectTimeout());
                ByteBufAllocator allocator =
                        env.poolBuffers() ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
                final Bootstrap bootstrap = new Bootstrap().option(ChannelOption.ALLOCATOR, allocator)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) env.socketConnectTimeout())
                        .remoteAddress(inetAddress.getHostString(), inetAddress.getPort())
                        .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
                        .handler(new DcpPipeline(env, controlHandler)).group(env.eventLoopGroup());
                ChannelFuture connectFuture = bootstrap.connect();
                connectFuture.await(2 * env.socketConnectTimeout());
                connectFuture.cancel(true);
                if (!connectFuture.isSuccess()) {
                    throw connectFuture.cause();
                }
                LOGGER.debug("Connection established");
                channel = connectFuture.channel();
                setState(State.CONNECTED);
            } catch (Throwable e) {
                LOGGER.warn("Connection failed", e);
                if (failure == null) {
                    failure = e;
                }
                if (attempts == env.dcpChannelsReconnectMaxAttempts()) {
                    LOGGER.warn("Connection FAILED " + attempts + " times");
                    channel = null;
                    setState(State.DISCONNECTED);
                    throw failure; // NOSONAR failure is not nullable
                }
            }
        }
        // attempt to restart the dropped streams
        for (int i = 0; i < openStreams.length; i++) {
            if (openStreams[i]) {
                LOGGER.debug("Opening a stream that was dropped for vbucket " + i);
                PartitionState ps = sessionState.get(i);
                ps.prepareNextStreamRequest();
                openStream((short) i, ps.getUuid(), ps.getSeqno(), SessionState.NO_END_SEQNO,
                        ps.getSnapshotStartSeqno(), ps.getSnapshotEndSeqno());
            }
        }
        for (int i = 0; i < failoverLogRequests.length; i++) {
            if (failoverLogRequests[i]) {
                LOGGER.debug("Re requesting failover logs for vbucket " + i);
                getFailoverLog((short) i);
            }
        }
        if (!stateFetched) {
            getSeqnos();
        }
        channel.closeFuture().addListener(closeListener);
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

    public synchronized void disconnect(boolean wait) throws InterruptedException {
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
        if (wait) {
            wait(State.DISCONNECTED);
        }
        channel = null;
    }

    public synchronized void openStream(final short vbid, final long vbuuid, final long startSeqno, final long endSeqno,
            final long snapshotStartSeqno, final long snapshotEndSeqno) {
        LOGGER.debug("opening stream for " + vbid);
        if (getState() != State.CONNECTED) {
            PartitionState state = sessionState.get(vbid);
            StreamEndEvent endEvent = state.getEndEvent();
            endEvent.setReason(StreamEndReason.CHANNEL_DROPPED);
            LOGGER.warn("Attempt to open stream on disconnected channel");
            env.eventBus().publish(endEvent);
        }
        LOGGER.debug(
                "Opening Stream against {} with vbid: {}, vbuuid: {}, startSeqno: {}, "
                        + "endSeqno: {},  snapshotStartSeqno: {}, snapshotEndSeqno: {}",
                channel.remoteAddress(), vbid, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno);
        sessionState.get(vbid).setState(PartitionState.CONNECTING);
        openStreams[vbid] = true;
        int opaque = OPAQUE.incrementAndGet();
        ByteBuf buffer = Unpooled.buffer();
        DcpOpenStreamRequest.init(buffer, vbid);
        DcpOpenStreamRequest.opaque(buffer, opaque);
        DcpOpenStreamRequest.vbuuid(buffer, vbuuid);
        DcpOpenStreamRequest.startSeqno(buffer, startSeqno);
        DcpOpenStreamRequest.endSeqno(buffer, endSeqno);
        DcpOpenStreamRequest.snapshotStartSeqno(buffer, snapshotStartSeqno);
        DcpOpenStreamRequest.snapshotEndSeqno(buffer, snapshotEndSeqno);
        vbuckets.put(opaque, vbid);
        channel.writeAndFlush(buffer);
    }

    public synchronized void closeStream(final short vbid) {
        if (getState() != State.CONNECTED) {
            throw new NotConnectedException();
        }
        LOGGER.debug("Closing Stream against {} with vbid: {}", channel.remoteAddress(), vbid);
        sessionState.get(vbid).setState(PartitionState.DISCONNECTING);
        openStreams[vbid] = false;
        int opaque = OPAQUE.incrementAndGet();
        ByteBuf buffer = Unpooled.buffer();
        DcpCloseStreamRequest.init(buffer);
        DcpCloseStreamRequest.vbucket(buffer, vbid);
        DcpCloseStreamRequest.opaque(buffer, opaque);
        vbuckets.put(opaque, vbid);
        channel.writeAndFlush(buffer);
    }

    /**
     * Returns all seqnos for all vbuckets on that channel.
     *
     * @throws InterruptedException
     */
    public synchronized void getSeqnos() {
        stateFetched = false;
        if (getState() != State.CONNECTED) {
            for (int i = 0; i < openStreams.length; i++) {
                PartitionState ps = sessionState.get(i);
                if (!ps.isClientDisconnected()) {
                    ps.fail(new NotConnectedException());
                }
            }
            return;
        }
        int opaque = OPAQUE.incrementAndGet();
        ByteBuf buffer = Unpooled.buffer();
        DcpGetPartitionSeqnosRequest.init(buffer);
        DcpGetPartitionSeqnosRequest.opaque(buffer, opaque);
        DcpGetPartitionSeqnosRequest.vbucketState(buffer, VbucketState.ACTIVE);
        channel.writeAndFlush(buffer);
    }

    public synchronized void getFailoverLog(final short vbid) {
        LOGGER.debug("requesting failover logs for vbucket " + vbid);
        failoverLogRequests[vbid] = true;
        if (getState() != State.CONNECTED) {
            PartitionState ps = sessionState.get(vbid);
            if (!ps.isClientDisconnected()) {
                ps.fail(new NotConnectedException());
            }
            return;
        }
        int opaque = OPAQUE.incrementAndGet();
        ByteBuf buffer = Unpooled.buffer();
        DcpFailoverLogRequest.init(buffer);
        DcpFailoverLogRequest.opaque(buffer, opaque);
        DcpFailoverLogRequest.vbucket(buffer, vbid);
        vbuckets.put(opaque, vbid);
        channel.writeAndFlush(buffer);
        LOGGER.debug("Asked for failover log on {} for vbid: {}", channel.remoteAddress(), vbid);
    }

    public boolean streamIsOpen(short vbid) {
        return openStreams[vbid];
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
        return "{\"class\":\"" + this.getClass().getSimpleName() + "\", \"inetAddress\":\"" + inetAddress
                + "\", \"state\":\"" + state + "\"}";
    }

    public ClientEnvironment getEnv() {
        return env;
    }

    public Map<Integer, Short> getVbuckets() {
        return vbuckets;
    }

    public boolean[] openStreams() {
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

    public synchronized void clear() {
        vbuckets.clear();
    }

    public SessionState getSessionState() {
        return sessionState;
    }

    public boolean[] getFailoverLogRequests() {
        return failoverLogRequests;
    }

    public synchronized void stateFetched() {
        stateFetched = true;
        notifyAll();
    }

    public boolean isStateFetched() {
        return stateFetched;
    }
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final InetAddress inetAddress;
    private final Map<Integer, Short> vbuckets;
    private final boolean[] failoverLogRequests;
    private final boolean[] openStreams;
    private final SessionState sessionState;
    private final DcpChannelControlMessageHandler controlHandler;
    private volatile Channel channel;
    private final DcpChannelCloseListener closeListener;
    private volatile boolean stateFetched = true;

    public DcpChannel(InetAddress inetAddress, final ClientEnvironment env, final SessionState sessionState,
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
                ByteBufAllocator allocator =
                        env.poolBuffers() ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
                final Bootstrap bootstrap = new Bootstrap().option(ChannelOption.ALLOCATOR, allocator)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) env.socketConnectTimeout())
                        .remoteAddress(inetAddress, env.sslEnabled() ? env.dcpSslPort() : env.dcpDirectPort())
                        .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
                        .handler(new DcpPipeline(env, controlHandler)).group(env.eventLoopGroup());
                ChannelFuture connectFuture = bootstrap.connect();
                connectFuture.await();
                if (!connectFuture.isSuccess()) {
                    throw connectFuture.cause();
                }
                LOGGER.debug("Connection established");
                channel = connectFuture.channel();
                setState(State.CONNECTED);
            } catch (Throwable e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
                if (attempts == env.dcpChannelsReconnectMaxAttempts()) {
                    LOGGER.debug("Connection FAILED " + attempts + " times");
                    channel = null;
                    setState(State.DISCONNECTED);
                    throw failure; // NOSONAR failure is not nullable
                }
                // Wait between attempts
                synchronized (this) {
                    wait(200);
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
            getSeqnos(false);
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

    public synchronized void disconnect() throws InterruptedException {
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
            case DISCONNECTED:
                return;
            default:
                break;
        }
        wait(State.DISCONNECTED);
        channel = null;
    }

    public InetAddress hostname() {
        return inetAddress;
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
    public synchronized void getSeqnos(boolean waitForResults) throws InterruptedException {
        if (getState() != State.CONNECTED) {
            throw new NotConnectedException();
        }
        int opaque = OPAQUE.incrementAndGet();
        ByteBuf buffer = Unpooled.buffer();
        DcpGetPartitionSeqnosRequest.init(buffer);
        DcpGetPartitionSeqnosRequest.opaque(buffer, opaque);
        DcpGetPartitionSeqnosRequest.vbucketState(buffer, VbucketState.ACTIVE);
        stateFetched = false;
        channel.writeAndFlush(buffer);
        if (waitForResults) {
            while (!stateFetched) {
                this.wait();
            }
        }
    }

    public synchronized void getFailoverLog(final short vbid) {
        LOGGER.debug("requesting failover logs for vbucket " + vbid);
        if (getState() != State.CONNECTED) {
            throw new NotConnectedException();
        }
        int opaque = OPAQUE.incrementAndGet();
        ByteBuf buffer = Unpooled.buffer();
        DcpFailoverLogRequest.init(buffer);
        DcpFailoverLogRequest.opaque(buffer, opaque);
        DcpFailoverLogRequest.vbucket(buffer, vbid);
        vbuckets.put(opaque, vbid);
        channel.writeAndFlush(buffer);
        failoverLogRequests[vbid] = true;
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
        return "DcpChannel{inetAddress=" + inetAddress + '}';
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

    public InetAddress getInetAddress() {
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
}

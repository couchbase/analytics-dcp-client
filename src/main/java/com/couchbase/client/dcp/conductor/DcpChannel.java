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
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpBufferAckRequest;
import com.couchbase.client.dcp.message.DcpCloseStreamRequest;
import com.couchbase.client.dcp.message.DcpFailoverLogRequest;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosRequest;
import com.couchbase.client.dcp.message.DcpOpenStreamRequest;
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
    private final boolean ackEnabled;
    private volatile int ackCounter;
    private final int ackWatermark;
    private final Conductor conductor;
    private final DcpChannelControlMessageHandler controlHandler;
    private volatile Channel channel;

    private final DcpChannelConnectListener connectListener;
    private final DcpChannelCloseListener closeListener;

    public DcpChannel(InetAddress inetAddress, final ClientEnvironment env, final Conductor conductor) {
        setState(State.DISCONNECTED);
        this.inetAddress = inetAddress;
        this.env = env;
        this.conductor = conductor;
        this.vbuckets = new ConcurrentHashMap<>();
        this.failoverLogRequests = new boolean[conductor.config().numberOfPartitions()];
        this.controlHandler = new DcpChannelControlMessageHandler(this);
        this.openStreams = new boolean[conductor.config().numberOfPartitions()];
        this.ackEnabled = env.dcpControl().ackEnabled();
        this.ackCounter = 0;
        if (ackEnabled) {
            int bufferAckPercent = env.ackWaterMark();
            int bufferSize = Integer.parseInt(env.dcpControl().get(DcpControl.Names.CONNECTION_BUFFER_SIZE));
            this.ackWatermark = (int) Math.round(bufferSize / 100.0 * bufferAckPercent);
            LOGGER.warn("BufferAckWatermark absolute is {}", ackWatermark);
        } else {
            this.ackWatermark = 0;
        }
        this.connectListener = new DcpChannelConnectListener(this);
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
                connectListener.listen(connectFuture);
                setState(State.CONNECTED);
            } catch (Throwable e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
                if (attempts == env.dcpChannelsReconnectMaxAttempts()) {
                    LOGGER.warn("Connection FAILED " + attempts + " times");
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
                LOGGER.warn("Opening a stream that was dropped for vbucket " + i);
                PartitionState ps = conductor.sessionState().get(i);
                ps.prepareNextStreamRequest();
                openStream((short) i, ps.getFailoverLog().get(0).getUuid(), ps.getSeqno(), SessionState.NO_END_SEQNO,
                        ps.getSnapshotStartSeqno(), ps.getSnapshotEndSeqno());
            }
        }
        for (int i = 0; i < failoverLogRequests.length; i++) {
            if (failoverLogRequests[i]) {
                LOGGER.warn("Re requesting failover logs for vbucket " + i);
                getFailoverLog((short) i);
            }
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
                setState(State.DISCONNECTING);
                channel.close();
                break;
            case DISCONNECTED:
                return;
            default:
                break;
        }
        ackCounter = 0;
        wait(State.DISCONNECTED);
    }

    public InetAddress hostname() {
        return inetAddress;
    }

    public synchronized void acknowledgeBuffer(final int numBytes) {
        if (getState() != State.CONNECTED) {
            return;
        }
        LOGGER.trace("Acknowledging {} bytes against connection {}.", numBytes, channel.remoteAddress());
        ackCounter += numBytes;
        LOGGER.trace("BufferAckCounter is now {}", ackCounter);
        if (ackCounter >= ackWatermark) {
            LOGGER.trace("BufferAckWatermark reached on {}, acking now against the server.", channel.remoteAddress());
            ByteBuf buffer = Unpooled.buffer();
            DcpBufferAckRequest.init(buffer);
            DcpBufferAckRequest.ackBytes(buffer, ackCounter);
            channel.writeAndFlush(buffer);
            ackCounter = 0;
        }
    }

    public synchronized void openStream(final short vbid, final long vbuuid, final long startSeqno,
            final long endSeqno, final long snapshotStartSeqno, final long snapshotEndSeqno) {
        LOGGER.warn("opening stream for " + vbid);
        if (getState() != State.CONNECTED) {
            throw new NotConnectedException();
        }
        LOGGER.debug(
                "Opening Stream against {} with vbid: {}, vbuuid: {}, startSeqno: {}, "
                        + "endSeqno: {},  snapshotStartSeqno: {}, snapshotEndSeqno: {}",
                channel.remoteAddress(), vbid, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno);
        conductor.getSessionState().get(vbid).setState(PartitionState.CONNECTING);
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
        conductor.getSessionState().get(vbid).setState(PartitionState.DISCONNECTING);
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
     */
    public synchronized void getSeqnos() {
        if (getState() != State.CONNECTED) {
            throw new NotConnectedException();
        }
        int opaque = OPAQUE.incrementAndGet();
        ByteBuf buffer = Unpooled.buffer();
        DcpGetPartitionSeqnosRequest.init(buffer);
        DcpGetPartitionSeqnosRequest.opaque(buffer, opaque);
        DcpGetPartitionSeqnosRequest.vbucketState(buffer, VbucketState.ACTIVE);
        channel.writeAndFlush(buffer);
    }

    public synchronized void getFailoverLog(final short vbid) {
        LOGGER.warn("requesting failover logs for vbucket " + vbid);
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
        if (o instanceof InetAddress) {
            return inetAddress.equals(o);
        } else if (o instanceof DcpChannel) {
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

    public boolean ackEnabled() {
        return ackEnabled;
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

    public Conductor getConductor() {
        return conductor;
    }

    public boolean[] getFailoverLogRequests() {
        return failoverLogRequests;
    }
}

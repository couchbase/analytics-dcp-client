/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import static com.couchbase.client.dcp.message.MessageUtil.REQ_DCP_DELETION;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_DCP_EXPIRATION;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_DCP_MUTATION;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_OSO_SNAPSHOT_MARKER;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_SEQNO_ADVANCED;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_SET_VBUCKET_STATE;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_SNAPSHOT_MARKER;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_STREAM_END;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_SYSTEM_EVENT;
import static com.couchbase.client.dcp.message.MessageUtil.RES_FAILOVER_LOG;
import static com.couchbase.client.dcp.message.MessageUtil.RES_GET_COLLECTIONS_MANIFEST;
import static com.couchbase.client.dcp.message.MessageUtil.RES_GET_SEQNOS;
import static com.couchbase.client.dcp.message.MessageUtil.RES_STREAM_CLOSE;
import static com.couchbase.client.dcp.message.MessageUtil.RES_STREAM_REQUEST;

import java.io.IOException;

import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.DcpAckHandle;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpBufferAckRequest;
import com.couchbase.client.dcp.message.DcpNoopRequest;
import com.couchbase.client.dcp.message.DcpNoopResponse;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil;

/**
 * Handles the "business logic" of incoming DCP mutation and control messages.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpMessageHandler extends ChannelDuplexHandler implements DcpAckHandle {

    private final ClientEnvironment env;

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpMessageHandler.class);

    /**
     * The data callback where the events are fed to the user.
     */
    private final DataEventHandler dataEventHandler;
    private final Channel channel;

    /**
     * The subject for the control events since they need more advanced handling up the stack.
     */
    private final ControlEventHandler controlEventHandler;
    private final boolean ackEnabled;
    private int ackCounter;
    private final int ackWatermark;
    private final DcpChannel dcpChannel;
    private final ChannelFutureListener ackListener;

    /**
     * Create a new message handler.
     *
     * @param ch
     *
     * @param env
     *
     * @param dataEventHandler
     *            data event callback handler.
     * @param controlEventHandler
     *            control event subject.
     */
    DcpMessageHandler(DcpChannel dcpChannel, Channel ch, ClientEnvironment env, final DataEventHandler dataEventHandler,
            final ControlEventHandler controlEventHandler) {
        this.dcpChannel = dcpChannel;
        this.channel = ch;
        this.env = env;
        this.dataEventHandler = dataEventHandler;
        this.controlEventHandler = controlEventHandler;
        this.ackEnabled = env.dcpControl().ackEnabled();
        this.ackCounter = 0;
        if (ackEnabled) {
            int bufferAckPercent = env.ackWaterMark();
            int bufferSize = Integer.parseInt(env.dcpControl().get(DcpControl.Names.CONNECTION_BUFFER_SIZE));
            this.ackWatermark = (int) Math.round(bufferSize / 100.0 * bufferAckPercent);
            LOGGER.debug("BufferAckWatermark absolute is {}", ackWatermark);
            ackListener = future -> {
                if (!future.isSuccess()) {
                    LOGGER.log(CouchbaseLogLevel.WARN, "Failed to send the ack to the dcp producer", future.cause());
                    ch.close();
                } else {
                    env.flowControlCallback().ackFlushedThroughNetwork(this, this.dcpChannel);
                }
            };
        } else {
            this.ackWatermark = 0;
            ackListener = null;
        }
    }

    /**
     * Dispatch every incoming message to either the data or the control feeds.
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        dcpChannel.newMessageRecieved();
        ByteBuf message = (ByteBuf) msg;
        if (isDataMessage(message)) {
            dataEventHandler.onEvent(this, message);
        } else if (isControlMessage(message)) {
            controlEventHandler.onEvent(this, message);
        } else {
            // We handle the message here and we're responsible for releasing it
            try {
                if (DcpNoopRequest.is(message)) {
                    ByteBuf buffer = ctx.alloc().buffer();
                    DcpNoopResponse.init(buffer);
                    MessageUtil.setOpaque(MessageUtil.getOpaque(message), buffer);
                    LOGGER.info("Sending back a NoOp response" + dcpChannel + ". Current ack counter = " + ackCounter);
                    ctx.writeAndFlush(buffer);
                } else {
                    LOGGER.warn("Unknown DCP Message, ignoring. \n{}", MessageUtil.humanize(message));
                }
            } finally {
                ReferenceCountUtil.release(message);
            }
        }
    }

    /**
     * Helper method to check if the given byte buffer is a control message.
     *
     * @param msg
     *            the message to check.
     * @return true if it is, false otherwise.
     */
    private static boolean isControlMessage(final ByteBuf msg) {
        switch (msg.getShort(0)) {
            case REQ_STREAM_END:
            case REQ_SNAPSHOT_MARKER:
            case REQ_SET_VBUCKET_STATE:
            case REQ_OSO_SNAPSHOT_MARKER:
            case REQ_SYSTEM_EVENT:
            case REQ_SEQNO_ADVANCED:
            case RES_STREAM_REQUEST:
            case RES_FAILOVER_LOG:
            case RES_STREAM_CLOSE:
            case RES_GET_SEQNOS:
            case RES_GET_COLLECTIONS_MANIFEST:
                return true;
            default:
                return false;
        }
    }

    /**
     * Helper method to check if the given byte buffer is a data message.
     *
     * @param msg
     *            the message to check.
     * @return true if it is, false otherwise.
     */
    private static boolean isDataMessage(final ByteBuf msg) {
        switch (msg.getShort(0)) {
            case REQ_DCP_MUTATION:
            case REQ_DCP_DELETION:
            case REQ_DCP_EXPIRATION:
                return true;
            default:
                return false;
        }
    }

    @Override
    public synchronized void ack(ByteBuf message) {
        if (!ackEnabled) {
            return;
        }
        switch (message.getShort(0)) {
            case REQ_SET_VBUCKET_STATE:
            case REQ_SNAPSHOT_MARKER:
            case REQ_STREAM_END:
            case RES_STREAM_CLOSE:
            case REQ_DCP_MUTATION:
            case REQ_DCP_DELETION:
            case REQ_DCP_EXPIRATION:
            case REQ_OSO_SNAPSHOT_MARKER:
                ackCounter += message.readableBytes();
                boolean trace = LOGGER.isTraceEnabled();
                if (trace) {
                    LOGGER.trace("BufferAckCounter is now {}", ackCounter);
                }
                if (ackCounter >= ackWatermark) {
                    env.flowControlCallback().bufferAckWaterMarkReached(this, dcpChannel, ackCounter, ackWatermark);
                    if (trace) {
                        LOGGER.trace("BufferAckWatermark reached on {}, acking now against the server.",
                                channel.remoteAddress());
                    }
                    ByteBuf buffer = channel.alloc().buffer();
                    DcpBufferAckRequest.init(buffer);
                    DcpBufferAckRequest.ackBytes(buffer, ackCounter);
                    ChannelFuture future = channel.writeAndFlush(buffer);
                    future.addListener(ackListener);
                    ackCounter = 0;
                }
                if (trace) {
                    LOGGER.trace("Acknowledging {} bytes against connection {}.", message.readableBytes(),
                            channel.remoteAddress());
                }
                break;
            default:
                // no-op for non-ACKable messages
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException && cause.getMessage().contains("Connection reset by peer")) {
            LOGGER.log(CouchbaseLogLevel.WARN, "Connection was closed by the other side", cause);
            ctx.close();
            return;
        }
        // forward exception
        ctx.fireExceptionCaught(cause);
    }

    public DcpChannel getDcpChannel() {
        return dcpChannel;
    }
}

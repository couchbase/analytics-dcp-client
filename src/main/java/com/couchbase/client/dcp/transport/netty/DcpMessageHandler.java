/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

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
import com.couchbase.client.dcp.message.DcpCloseStreamResponse;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosResponse;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpNoopRequest;
import com.couchbase.client.dcp.message.DcpNoopResponse;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpStateVbucketStateMessage;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;

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
     * @param controlHandler
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
            LOGGER.warn("BufferAckWatermark absolute is {}", ackWatermark);
            ackListener = future -> {
                if (!future.isSuccess()) {
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
        } else if (DcpNoopRequest.is(message)) {
            ByteBuf buffer = ctx.alloc().buffer();
            DcpNoopResponse.init(buffer);
            MessageUtil.setOpaque(MessageUtil.getOpaque(message), buffer);
            LOGGER.info("Sending back a NoOp response" + dcpChannel);
            ctx.writeAndFlush(buffer);
        } else {
            LOGGER.warn("Unknown DCP Message, ignoring. \n{}", MessageUtil.humanize(message));
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
        return DcpStateVbucketStateMessage.is(msg) || DcpOpenStreamResponse.is(msg) || DcpStreamEndMessage.is(msg)
                || DcpSnapshotMarkerRequest.is(msg) || DcpFailoverLogResponse.is(msg) || DcpCloseStreamResponse.is(msg)
                || DcpGetPartitionSeqnosResponse.is(msg);
    }

    /**
     * Helper method to check if the given byte buffer is a data message.
     *
     * @param msg
     *            the message to check.
     * @return true if it is, false otherwise.
     */
    private static boolean isDataMessage(final ByteBuf msg) {
        return DcpMutationMessage.is(msg) || DcpDeletionMessage.is(msg) || DcpExpirationMessage.is(msg);
    }

    @Override
    public synchronized void ack(ByteBuf message) {
        if (ackEnabled && (DcpStateVbucketStateMessage.is(message) || DcpSnapshotMarkerRequest.is(message)
                || DcpStreamEndMessage.is(message) || DcpCloseStreamResponse.is(message)
                || DcpMutationMessage.is(message) || DcpDeletionMessage.is(message)
                || DcpExpirationMessage.is(message))) {
            ackCounter += message.readableBytes();
            LOGGER.trace("BufferAckCounter is now {}", ackCounter);
            if (ackCounter >= ackWatermark) {
                env.flowControlCallback().bufferAckWaterMarkReached(this, dcpChannel, ackCounter, ackWatermark);
                LOGGER.trace("BufferAckWatermark reached on {}, acking now against the server.",
                        channel.remoteAddress());
                ByteBuf buffer = channel.alloc().buffer();
                DcpBufferAckRequest.init(buffer);
                DcpBufferAckRequest.ackBytes(buffer, ackCounter);
                ChannelFuture future = channel.writeAndFlush(buffer);
                future.addListener(ackListener);
                ackCounter = 0;
            }
            LOGGER.trace("Acknowledging {} bytes against connection {}.", message.readableBytes(),
                    channel.remoteAddress());
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

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_DELETION_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_EXPIRATION_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_MUTATION_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_OSO_SNAPSHOT_MARKER_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_SET_VBUCKET_STATE_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_SNAPSHOT_MARKER_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_END_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_DCP_DELETION;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_DCP_EXPIRATION;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_DCP_MUTATION;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_OSO_SNAPSHOT_MARKER;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_SEQNO_ADVANCED;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_SET_VBUCKET_STATE;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_SNAPSHOT_MARKER;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_STREAM_END;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_SYSTEM_EVENT;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_DCP_DELETION;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_DCP_EXPIRATION;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_DCP_MUTATION;
import static com.couchbase.client.dcp.message.MessageUtil.REQ_DCP_NOOP;
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
     * Dispatch every incoming message to the appropriate feed based on opcode.
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        dcpChannel.newMessageRecieved();
        ByteBuf message = (ByteBuf) msg;
        switch (message.getShort(0)) {
            case REQ_DCP_MUTATION:
            case REQ_DCP_DELETION:
            case REQ_DCP_EXPIRATION:
            case FLEX_REQ_DCP_MUTATION:
            case FLEX_REQ_DCP_DELETION:
            case FLEX_REQ_DCP_EXPIRATION:
                dataEventHandler.onEvent(this, message);
                break;

            case REQ_STREAM_END:
            case REQ_SNAPSHOT_MARKER:
            case REQ_SET_VBUCKET_STATE:
            case REQ_OSO_SNAPSHOT_MARKER:
            case REQ_SYSTEM_EVENT:
            case REQ_SEQNO_ADVANCED:
            case FLEX_REQ_STREAM_END:
            case FLEX_REQ_SNAPSHOT_MARKER:
            case FLEX_REQ_SET_VBUCKET_STATE:
            case FLEX_REQ_OSO_SNAPSHOT_MARKER:
            case FLEX_REQ_SYSTEM_EVENT:
            case FLEX_REQ_SEQNO_ADVANCED:
            case RES_STREAM_REQUEST:
            case RES_FAILOVER_LOG:
            case RES_STREAM_CLOSE:
            case RES_GET_SEQNOS:
            case RES_GET_COLLECTIONS_MANIFEST:
                controlEventHandler.onEvent(this, message);
                break;

            case REQ_DCP_NOOP:
                // We handle the message here and we're responsible for releasing it
                try {
                    ByteBuf buffer = ctx.alloc().buffer();
                    DcpNoopResponse.init(buffer);
                    MessageUtil.setOpaque(MessageUtil.getOpaque(message), buffer);
                    LOGGER.info("Sending back a NoOp response" + dcpChannel + ". Current ack counter = " + ackCounter);
                    ctx.writeAndFlush(buffer);
                } finally {
                    ReferenceCountUtil.release(message);
                }
                break;

            default:
                try {
                    // TODO(mblow): consider only logging WARN once per opcode [per client/channel/jvm], to prevent log
                    //              blowout
                    LOGGER.warn("Unknown DCP Message, ignoring. \n{}", MessageUtil.humanize(message));
                } finally {
                    ReferenceCountUtil.release(message);
                }

        }
    }

    /**
     * Handles ACK of the supplied message to the DCP producer.  Note that this does not necessarily immediately
     * notify the producer, instead as the consumer we keep track of ACK bytes only notifying the producer once
     * we reach the configured watermark, which is a percentage of the configured connection buffer size.
     *
     * Per https://github.com/couchbase/kv_engine/blob/master/docs/dcp/documentation/flow-control.md#buffering-messages,
     * only Mutation, Deletion, Expiration, Snapshot Markers, OSO Snapshot Markers, Set VBucket State, and Stream End
     * messages should be buffered, so only these messages are ACK'd.
     *
     * @param message the DCP message to ACK, if applicable based on type
     */
    @Override
    public void ack(ByteBuf message) {
        if (!ackEnabled) {
            return;
        }
        int fixup = 0;
        switch (message.getByte(1)) {
            case DCP_OSO_SNAPSHOT_MARKER_OPCODE:
                // TODO: workaround for MB-42506 (remove once MB-42506 is fixed)
                fixup = -MessageUtil.getFramingExtrasSize(message);
                // fall-through
            case DCP_MUTATION_OPCODE:
            case DCP_DELETION_OPCODE:
            case DCP_EXPIRATION_OPCODE:
            case DCP_SNAPSHOT_MARKER_OPCODE:
            case DCP_SET_VBUCKET_STATE_OPCODE:
            case DCP_STREAM_END_OPCODE:
                final int ackBytes = message.readableBytes() + fixup;
                synchronized (this) {
                    ackCounter += ackBytes;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("BufferAckCounter is now {} after including {} for opcode 0x{}", ackCounter,
                                ackBytes, Integer.toUnsignedString(message.getUnsignedByte(1), 16));
                    }
                    if (ackCounter >= ackWatermark) {
                        env.flowControlCallback().bufferAckWaterMarkReached(this, dcpChannel, ackCounter, ackWatermark);
                        LOGGER.debug("BufferAckWatermark ({}) reached on {}, acking {} bytes now with the server",
                                ackWatermark, channel.remoteAddress(), ackCounter);
                        ByteBuf buffer = channel.alloc().buffer();
                        DcpBufferAckRequest.init(buffer);
                        DcpBufferAckRequest.ackBytes(buffer, ackCounter);
                        ChannelFuture future = channel.writeAndFlush(buffer);
                        future.addListener(ackListener);
                        ackCounter = 0;
                    }
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

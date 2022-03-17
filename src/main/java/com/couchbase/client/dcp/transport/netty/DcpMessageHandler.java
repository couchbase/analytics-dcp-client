/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.transport.netty;

import static com.couchbase.client.dcp.DcpAckHandle.Util.NOOP_ACK_HANDLE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_NOOP_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_DCP_DELETION;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_DCP_EXPIRATION;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_DCP_MUTATION;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_OSO_SNAPSHOT_MARKER;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_SEQNO_ADVANCED;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_SET_VBUCKET_STATE;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_SNAPSHOT_MARKER;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_STREAM_END;
import static com.couchbase.client.dcp.message.MessageUtil.FLEX_REQ_SYSTEM_EVENT;
import static com.couchbase.client.dcp.message.MessageUtil.MAGIC_REQ;
import static com.couchbase.client.dcp.message.MessageUtil.MAGIC_REQ_FLEX;
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
import static com.couchbase.client.dcp.message.MessageUtil.RES_DCP_COLLECTIONS_MANIFEST;
import static com.couchbase.client.dcp.message.MessageUtil.RES_FAILOVER_LOG;
import static com.couchbase.client.dcp.message.MessageUtil.RES_GET_SEQNOS;
import static com.couchbase.client.dcp.message.MessageUtil.RES_STAT;
import static com.couchbase.client.dcp.message.MessageUtil.RES_STREAM_CLOSE;
import static com.couchbase.client.dcp.message.MessageUtil.RES_STREAM_REQUEST;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.util.LogRedactionUtil;

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
import com.couchbase.client.dcp.message.DcpDataMessage;
import com.couchbase.client.dcp.message.DcpNoopResponse;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.DcpOsoSnapshotMarkerMessage;
import com.couchbase.client.dcp.message.DcpSeqnoAdvancedMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpSystemEventMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.CollectionsUtil;
import com.couchbase.client.dcp.util.MemcachedStatus;
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
    private final DcpAckHandle ackHandle;
    private int ackCounter;
    private final int ackWatermark;
    private final DcpChannel dcpChannel;
    private final ChannelFutureListener ackListener;
    private final String connectionId;

    private static boolean ackSanity;
    private static final Set<AckKey> globalPendingAck = Collections.newSetFromMap(new ConcurrentHashMap<>());

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
        this.connectionId = env.connectionNameGenerator().displayForm(dcpChannel.getConnectionName());
        if (ackEnabled) {
            int bufferAckPercent = env.ackWaterMark();
            int bufferSize = Integer.parseInt(env.dcpControl().get(DcpControl.Names.CONNECTION_BUFFER_SIZE));
            this.ackWatermark = (int) Math.round(bufferSize / 100.0 * bufferAckPercent);
            LOGGER.debug("BufferAckWatermark absolute is {}", ackWatermark);
            ackHandle = this;
            ackListener = future -> {
                if (!future.isSuccess()) {
                    LOGGER.log(CouchbaseLogLevel.WARN, "Failed to send the ack to the dcp producer", future.cause());
                    ch.close();
                } else {
                    env.flowControlCallback().ackFlushedThroughNetwork(ackHandle, this.dcpChannel);
                }
            };
            if (!ackSanity && LOGGER.isTraceEnabled()) {
                LOGGER.warn("{} ACK sanity checks enabled; violations will be logged here", connectionId);
                ackSanity = true; // NOSONAR: S3010 - assignment to static field in ctor
            }
        } else {
            this.ackWatermark = 0;
            ackHandle = NOOP_ACK_HANDLE;
            ackListener = null;
        }
    }

    /**
     * Dispatch every incoming message to the appropriate feed based on opcode.
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        dcpChannel.newMessageReceived();
        ByteBuf message = (ByteBuf) msg;
        if (LOGGER.isTraceEnabled()) {
            trace(message);
        }
        switch (message.getShort(0)) {
            case REQ_DCP_MUTATION:
            case REQ_DCP_DELETION:
            case REQ_DCP_EXPIRATION:
            case FLEX_REQ_DCP_MUTATION:
            case FLEX_REQ_DCP_DELETION:
            case FLEX_REQ_DCP_EXPIRATION:
                if (ackSanity && ackEnabled) {
                    globalPendingAck.add(AckKey.from(message));
                }
                dataEventHandler.onEvent(ackHandle, message);
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
                if (ackSanity && ackEnabled) {
                    globalPendingAck.add(AckKey.from(message));
                }
                // fall-through
            case RES_STREAM_REQUEST:
            case RES_FAILOVER_LOG:
            case RES_STREAM_CLOSE:
            case RES_GET_SEQNOS:
            case RES_DCP_COLLECTIONS_MANIFEST:
            case RES_STAT:
                controlEventHandler.onEvent(ackHandle, message);
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

    private void trace(final ByteBuf message) {
        switch (message.getShort(0)) {
            case FLEX_REQ_DCP_MUTATION:
            case FLEX_REQ_DCP_DELETION:
            case FLEX_REQ_DCP_EXPIRATION:
                LOGGER.trace("{} {} sid {} vbid {} seq {} cid {} key {}", connectionId,
                        MessageUtil.humanizeOpcode(message), MessageUtil.streamId(message, -1),
                        MessageUtil.getVbucket(message), DcpDataMessage.bySeqno(message),
                        CollectionsUtil.displayCid(MessageUtil.getCid(message)), LogRedactionUtil
                                .userData(MessageUtil.getKeyAsString(message, dcpChannel.isCollectionCapable())));
                break;
            case REQ_DCP_MUTATION:
            case REQ_DCP_DELETION:
            case REQ_DCP_EXPIRATION:
                LOGGER.trace("{} {} vbid {} seq {} cid {} key {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.getVbucket(message), DcpDataMessage.bySeqno(message),
                        CollectionsUtil.displayCid(MessageUtil.getCid(message)), LogRedactionUtil
                                .userData(MessageUtil.getKeyAsString(message, dcpChannel.isCollectionCapable())));
                break;
            case FLEX_REQ_OSO_SNAPSHOT_MARKER:
                LOGGER.trace("{} {} sid {} vbid {} flags {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.streamId(message, -1), MessageUtil.getVbucket(message),
                        DcpOsoSnapshotMarkerMessage.humanizeFlags(message));
                break;
            case FLEX_REQ_SNAPSHOT_MARKER:
                LOGGER.trace("{} {} sid {} vbid {} startseq {} endseq {}", connectionId,
                        MessageUtil.humanizeOpcode(message), MessageUtil.streamId(message, -1),
                        MessageUtil.getVbucket(message), DcpSnapshotMarkerRequest.startSeqno(message),
                        DcpSnapshotMarkerRequest.endSeqno(message));
                break;
            case FLEX_REQ_STREAM_END:
            case FLEX_REQ_SET_VBUCKET_STATE:
                LOGGER.trace("{} {} sid {} vbid {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.streamId(message, -1), MessageUtil.getVbucket(message));
                break;
            case FLEX_REQ_SYSTEM_EVENT:
                LOGGER.trace("{} {} sid {} vbid {} seq {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.streamId(message, -1), MessageUtil.getVbucket(message),
                        DcpSystemEventMessage.seqno(message));
                break;
            case FLEX_REQ_SEQNO_ADVANCED:
                LOGGER.trace("{} {} sid {} vbid {} seq {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.streamId(message, -1), MessageUtil.getVbucket(message),
                        DcpSeqnoAdvancedMessage.getSeqno(message));
                break;
            case RES_STREAM_REQUEST:
                LOGGER.trace("{} {} sid {} vbid {} status {}{}", connectionId, MessageUtil.humanizeOpcode(message),
                        DcpOpenStreamResponse.streamId(message), DcpOpenStreamResponse.vbucket(message),
                        MemcachedStatus.toString(MessageUtil.getStatus(message)),
                        (MessageUtil.getStatus(message) == MemcachedStatus.ROLLBACK
                                ? "rollbackseq " + DcpOpenStreamResponse.rollbackSeqno(message) : ""));
                break;
            case REQ_SNAPSHOT_MARKER:
                LOGGER.trace("{} {} vbid {} startseq {} endseq {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.getVbucket(message), DcpSnapshotMarkerRequest.startSeqno(message),
                        DcpSnapshotMarkerRequest.endSeqno(message));
                break;
            case REQ_SYSTEM_EVENT:
                LOGGER.trace("{} {} vbid {} seq {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.getVbucket(message), DcpSystemEventMessage.seqno(message));
                break;
            case REQ_SEQNO_ADVANCED:
                LOGGER.trace("{} {} vbid {} seq {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.getVbucket(message), DcpSeqnoAdvancedMessage.getSeqno(message));
                break;
            case REQ_STREAM_END:
            case REQ_SET_VBUCKET_STATE:
            case REQ_OSO_SNAPSHOT_MARKER:
            case RES_STREAM_CLOSE:
            case RES_GET_SEQNOS:
            case RES_DCP_COLLECTIONS_MANIFEST:
                LOGGER.trace("{} {} vbid {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.getVbucket(message));
                break;
            case RES_FAILOVER_LOG:
                LOGGER.trace("{} {} vbid {} status {}", connectionId, MessageUtil.humanizeOpcode(message),
                        MessageUtil.getOpaque(message), MemcachedStatus.toString(MessageUtil.getStatus(message)));
                break;
            case REQ_DCP_NOOP:
                LOGGER.trace("{} {}", connectionId, MessageUtil.humanizeOpcode(message));
                break;
            default:
                // no-op
        }
    }

    /**
     * Handles ACK of the supplied message to the DCP producer.  Note that this does not necessarily immediately
     * notify the producer, instead as the consumer we keep track of ACK bytes only notifying the producer once
     * we reach the configured watermark, which is a percentage of the configured connection buffer size.
     *
     * Per https://github.com/couchbase/kv_engine/blob/master/docs/dcp/documentation/flow-control.md#what-messages-must-be-acknowledged-by-dcp-clients,
     * "Every DCP request message the server sends except for no-op requires acknowledgement", so only these messages
     * are ACK'd.
     *
     * @param message the DCP message to ACK, if applicable based on type
     */
    @Override
    public void ack(ByteBuf message) {
        if (!ackEnabled) {
            return;
        }
        final byte magicByte = message.getByte(0);
        if (magicByte == MAGIC_REQ || magicByte == MAGIC_REQ_FLEX) {
            if (ackSanity) {
                final AckKey ackKey = AckKey.from(message);
                if (!globalPendingAck.remove(ackKey)) {
                    LOGGER.warn("acking non-pending message! key={} message={} stack={}", ackKey,
                            MessageUtil.humanize(message), Arrays.toString(new Throwable().getStackTrace()));
                } else {
                    LOGGER.debug("acking pending message {}", ackKey);
                }
            }
            // we should never get called on a NOOP
            if (message.getByte(1) == DCP_NOOP_OPCODE) {
                throw new IllegalStateException("ack() called on NOOP");
            }
            final int ackBytes = message.readableBytes();
            synchronized (ackHandle) {
                ackCounter += ackBytes;
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("{} BufferAckCounter is now {} after += {} for opcode {}", connectionId, ackCounter,
                            ackBytes, MessageUtil.humanizeOpcode(message));
                }
                if (ackCounter >= ackWatermark) {
                    env.flowControlCallback().bufferAckWaterMarkReached(ackHandle, dcpChannel, ackCounter,
                            ackWatermark);
                    LOGGER.debug("{} BufferAckWatermark ({}) reached on {}, acking {} bytes now with the server",
                            connectionId, ackWatermark, channel.remoteAddress(), ackCounter);
                    ByteBuf buffer = channel.alloc().buffer();
                    DcpBufferAckRequest.init(buffer);
                    DcpBufferAckRequest.ackBytes(buffer, ackCounter);
                    ChannelFuture future = channel.writeAndFlush(buffer);
                    future.addListener(ackListener);
                    ackCounter = 0;
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException && cause.getMessage().contains("Connection reset by peer")) {
            LOGGER.log(CouchbaseLogLevel.WARN, "{} connection was closed by the other side", connectionId, cause);
            ctx.close();
            return;
        }
        // forward exception
        ctx.fireExceptionCaught(cause);
    }

    public DcpChannel getDcpChannel() {
        return dcpChannel;
    }

    public static boolean release(ByteBuf buffer) {
        if (ackSanity && !globalPendingAck.isEmpty()) {
            try {
                final AckKey ackKey = AckKey.from(buffer);
                if (globalPendingAck.remove(ackKey)) {
                    LOGGER.warn("released pending ack: key={} message={} stack={}", ackKey,
                            MessageUtil.humanize(buffer), Arrays.toString(new Throwable().getStackTrace()));
                }
            } catch (Throwable t) {
                LOGGER.debug("ignoring exception logging pending ack", t);
            }
        }
        return ReferenceCountUtil.release(buffer);
    }

    public static boolean ackAndRelease(DcpAckHandle handle, ByteBuf msg) {
        handle.ack(msg);
        return release(msg);
    }

    private static class AckKey {
        private final int messageHash;
        private final byte messageOpcode;
        private final int messageReadableBytes;

        AckKey(ByteBuf message) {
            this.messageHash = System.identityHashCode(message);
            this.messageOpcode = message.getByte(1);
            this.messageReadableBytes = message.readableBytes();
        }

        static AckKey from(ByteBuf message) {
            return new AckKey(message);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AckKey ackKey = (AckKey) o;
            return messageHash == ackKey.messageHash && messageOpcode == ackKey.messageOpcode
                    && messageReadableBytes == ackKey.messageReadableBytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(messageHash, messageOpcode, messageReadableBytes);
        }

        @Override
        public String toString() {
            return "{" + "address=0x" + Integer.toHexString(messageHash) + ", opcode="
                    + MessageUtil.humanizeOpcode(messageOpcode) + ", readableBytes=" + messageReadableBytes + '}';
        }
    }
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.events.RollbackEvent;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpCloseStreamResponse;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosResponse;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

public class DcpChannelControlMessageHandler implements ControlEventHandler {
    private static final CouchbaseLogger LOGGER =
            CouchbaseLoggerFactory.getInstance(DcpChannelControlMessageHandler.class.getName());
    private final DcpChannel channel;

    public DcpChannelControlMessageHandler(DcpChannel channel) {
        this.channel = channel;
    }

    @Override
    public void onEvent(ByteBuf buf) {
        if (DcpOpenStreamResponse.is(buf)) {
            handleOpenStreamResponse(buf);
        } else if (DcpFailoverLogResponse.is(buf)) {
            handleFailoverLogResponse(buf);
        } else if (DcpStreamEndMessage.is(buf)) {
            handleDcpStreamEndMessage(buf);
        } else if (DcpCloseStreamResponse.is(buf)) {
            handleDcpCloseStreamResponse(buf);
        } else if (DcpGetPartitionSeqnosResponse.is(buf)) {
            handleDcpGetPartitionSeqnosResponse(buf);
        } else if (DcpSnapshotMarkerRequest.is(buf)) {
            handleDcpSnapshotMarker(buf);
        }
    }

    private void handleDcpSnapshotMarker(ByteBuf buf) {
        try {
            short vbucket = DcpSnapshotMarkerRequest.partition(buf);
            long start = DcpSnapshotMarkerRequest.startSeqno(buf);
            long end = DcpSnapshotMarkerRequest.endSeqno(buf);
            PartitionState ps = channel.getConductor().getSessionState().get(vbucket);
            ps.useStreamRequest();
            ps.setSnapshotStartSeqno(start);
            ps.setSnapshotEndSeqno(end);
            if (channel.ackEnabled()) {
                channel.acknowledgeBuffer(buf.readableBytes());
            }
        } finally {
            buf.release();
        }
    }

    private void handleOpenStreamResponse(ByteBuf buf) {
        try {
            short vbid = channel.getVbuckets().remove(MessageUtil.getOpaque(buf));
            short status = MessageUtil.getStatus(buf);
            switch (status) {
                case 0x00:
                    LOGGER.warn("stream opened successfully for vbucket " + vbid);
                    handleOpenStreamSuccess(buf, vbid);
                    break;
                case 0x23:
                    LOGGER.warn("stream rollback response for vbucket " + vbid);
                    handleOpenStreamRollback(buf, vbid);
                    break;
                case 0x07:
                    LOGGER.warn("stream not my vbucket response for vbucket " + vbid);
                    channel.openStreams()[vbid] = false;
                    channel.getConductor().getSessionState().get(vbid).setState(PartitionState.DISCONNECTED);
                    channel.getEnv().eventBus().publish(new NotMyVBuvketEvent(channel, vbid));
                    break;
                default:
                    channel.openStreams()[vbid] = false;
                    channel.getConductor().getSessionState().get(vbid).setState(PartitionState.DISCONNECTED);
                    LOGGER.warn("stream unknown response for vbucket " + vbid);
            }
        } finally {
            buf.release();
        }
    }

    private void handleFailoverLogResponse(ByteBuf buf) {
        try {
            short vbid = channel.getVbuckets().remove(MessageUtil.getOpaque(buf));
            ByteBuf flog = Unpooled.buffer();
            DcpFailoverLogResponse.init(flog);
            DcpFailoverLogResponse.vbucket(flog, DcpFailoverLogResponse.vbucket(buf));
            ByteBuf copiedBuf = MessageUtil.getContent(buf).copy().writeShort(vbid);
            MessageUtil.setContent(copiedBuf, flog);
            copiedBuf.release();
            PartitionState ps = channel.getConductor().getSessionState().get(vbid);
            DcpFailoverLogResponse.fill(flog, ps.getFailoverLog());
            channel.getFailoverLogRequests()[vbid] = false;
            ps.failoverUpdated();
        } finally {
            buf.release();
        }
    }

    private void handleOpenStreamRollback(ByteBuf buf, short vbid) {
        channel.openStreams()[vbid] = false;
        channel.getConductor().getSessionState().get(vbid).setState(PartitionState.DISCONNECTED);
        channel.getEnv().eventBus().publish(new RollbackEvent(vbid, DcpOpenStreamResponse.rollbackSeqno(buf)));
    }

    private void handleOpenStreamSuccess(ByteBuf buf, short vbid) {
        channel.openStreams()[vbid] = true;
        channel.getConductor().getSessionState().get(vbid).setState(PartitionState.CONNECTED);
        ByteBuf flog = Unpooled.buffer();
        DcpFailoverLogResponse.init(flog);
        DcpFailoverLogResponse.vbucket(flog, DcpOpenStreamResponse.vbucket(buf));
        ByteBuf content = MessageUtil.getContent(buf).copy().writeShort(vbid);
        MessageUtil.setContent(content, flog);
        content.release();
        channel.getEnv().controlEventHandler().onEvent(flog);
    }

    private void handleDcpGetPartitionSeqnosResponse(ByteBuf buf) {
        try {
            ByteBuf content = MessageUtil.getContent(buf);
            int size = content.readableBytes() / 10;
            for (int i = 0; i < size; i++) {
                int offset = i * 10;
                short vbid = content.getShort(offset);
                long seq = content.getLong(offset + Short.BYTES);
                channel.getConductor().sessionState().get(vbid).setCurrentVBucketSeqnoInMaster(seq);
            }
        } finally {
            buf.release();
        }
    }

    private void handleDcpStreamEndMessage(ByteBuf buf) {
        try {
            short vbid = DcpStreamEndMessage.vbucket(buf);
            StreamEndReason reason = DcpStreamEndMessage.reason(buf);
            LOGGER.warn("Server closed Stream on vbid {} with reason {}", vbid, reason);
            if (channel.getEnv().eventBus() != null) {
                channel.getEnv().eventBus().publish(new StreamEndEvent(vbid, reason));
            }
            channel.openStreams()[vbid] = false;

            if (channel.ackEnabled()) {
                channel.acknowledgeBuffer(buf.readableBytes());
            }
        } finally {
            buf.release();
        }
    }

    private void handleDcpCloseStreamResponse(ByteBuf buf) {
        try {
            Short vbid = channel.getVbuckets().remove(MessageUtil.getOpaque(buf));
            if (channel.ackEnabled()) {
                channel.acknowledgeBuffer(buf.readableBytes());
            }
            channel.openStreams()[vbid] = false;
            channel.getConductor().getSessionState().get(vbid).setState(PartitionState.DISCONNECTED);
            LOGGER.warn("Closed Stream against {} with vbid: {}", channel.getInetAddress(), vbid);
        } finally {
            buf.release();
        }
    }
}

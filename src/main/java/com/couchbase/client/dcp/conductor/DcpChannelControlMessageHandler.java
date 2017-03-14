/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.events.NotMyVBuvketEvent;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpCloseStreamResponse;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosResponse;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpStateVbucketStateMessage;
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
        } else if (DcpStateVbucketStateMessage.is(buf)) {
            handleDcpStateVbucketStateMessage(buf);
        }
    }

    private void handleDcpStateVbucketStateMessage(ByteBuf buf) {
        // this should only happen between kv nodes
    }

    private void handleDcpSnapshotMarker(ByteBuf buf) {
        try {
            short vbucket = DcpSnapshotMarkerRequest.partition(buf);
            long start = DcpSnapshotMarkerRequest.startSeqno(buf);
            long end = DcpSnapshotMarkerRequest.endSeqno(buf);
            PartitionState ps = channel.getSessionState().get(vbucket);
            ps.useStreamRequest();
            ps.setSnapshotStartSeqno(start);
            ps.setSnapshotEndSeqno(end);
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
                    LOGGER.debug("stream opened successfully for vbucket " + vbid);
                    handleOpenStreamSuccess(buf, vbid);
                    break;
                case 0x23:
                    LOGGER.debug("stream rollback response for vbucket " + vbid);
                    handleOpenStreamRollback(buf, vbid);
                    break;
                case 0x07:
                    LOGGER.debug("stream not my vbucket response for vbucket " + vbid);
                    channel.openStreams()[vbid] = false;
                    channel.getSessionState().get(vbid).setState(PartitionState.DISCONNECTED);
                    channel.getEnv().eventBus().publish(new NotMyVBuvketEvent(channel, vbid));
                    break;
                default:
                    channel.openStreams()[vbid] = false;
                    channel.getSessionState().get(vbid).setState(PartitionState.DISCONNECTED);
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
            PartitionState ps = channel.getSessionState().get(vbid);
            DcpFailoverLogResponse.fill(flog, ps);
            channel.getFailoverLogRequests()[vbid] = false;
            ps.failoverUpdated();
            channel.getEnv().eventBus().publish(ps.getFailoverLogUpdateEvent());
        } finally {
            buf.release();
        }
    }

    private void handleOpenStreamRollback(ByteBuf buf, short vbid) {
        channel.openStreams()[vbid] = false;
        PartitionState partitionState = channel.getSessionState().get(vbid);
        partitionState.setState(PartitionState.DISCONNECTED);
        partitionState.getRollbackEvent().setSeq(DcpOpenStreamResponse.rollbackSeqno(buf));
        channel.getEnv().eventBus().publish(partitionState.getRollbackEvent());
    }

    private void handleOpenStreamSuccess(ByteBuf buf, short vbid) {
        channel.openStreams()[vbid] = true;
        channel.getSessionState().get(vbid).setState(PartitionState.CONNECTED);
        ByteBuf flog = Unpooled.buffer();
        DcpFailoverLogResponse.init(flog);
        DcpFailoverLogResponse.vbucket(flog, DcpOpenStreamResponse.vbucket(buf));
        ByteBuf content = MessageUtil.getContent(buf).copy().writeShort(vbid);
        MessageUtil.setContent(content, flog);
        content.release();
    }

    private void handleDcpGetPartitionSeqnosResponse(ByteBuf buf) {
        try {
            ByteBuf content = MessageUtil.getContent(buf);
            int size = content.readableBytes() / 10;
            for (int i = 0; i < size; i++) {
                int offset = i * 10;
                short vbid = content.getShort(offset);
                long seq = content.getLong(offset + Short.BYTES);
                channel.getSessionState().get(vbid).setCurrentVBucketSeqnoInMaster(seq);
            }
        } finally {
            buf.release();
        }
    }

    private void handleDcpStreamEndMessage(ByteBuf buf) {
        try {
            short vbid = DcpStreamEndMessage.vbucket(buf);
            channel.openStreams()[vbid] = false;
            StreamEndReason reason = DcpStreamEndMessage.reason(buf);
            PartitionState state = channel.getSessionState().get(vbid);
            StreamEndEvent endEvent = state.getEndEvent();
            endEvent.setReason(reason);
            LOGGER.debug("Server closed Stream on vbid {} with reason {}", vbid, reason);
            if (channel.getEnv().eventBus() != null) {
                channel.getEnv().eventBus().publish(endEvent);
            }
        } finally {
            buf.release();
        }
    }

    private void handleDcpCloseStreamResponse(ByteBuf buf) {
        try {
            Short vbid = channel.getVbuckets().remove(MessageUtil.getOpaque(buf));
            channel.openStreams()[vbid] = false;
            channel.getSessionState().get(vbid).setState(PartitionState.DISCONNECTED);
            LOGGER.debug("Closed Stream against {} with vbid: {}", channel.getInetAddress(), vbid);
        } finally {
            buf.release();
        }
    }
}

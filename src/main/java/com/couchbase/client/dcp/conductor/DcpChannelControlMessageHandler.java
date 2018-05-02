/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DcpAckHandle;
import com.couchbase.client.dcp.events.OpenStreamResponse;
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
import com.couchbase.client.dcp.util.MemcachedStatus;
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
    public void onEvent(DcpAckHandle ackHandle, ByteBuf buf) {
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
        channel.getEnv().controlEventHandler().onEvent(ackHandle, buf);
    }

    private void handleDcpStateVbucketStateMessage(ByteBuf buf) {
        // this should only happen between kv nodes
    }

    private void handleDcpSnapshotMarker(ByteBuf buf) {
        short vbucket = DcpSnapshotMarkerRequest.partition(buf);
        long start = DcpSnapshotMarkerRequest.startSeqno(buf);
        long end = DcpSnapshotMarkerRequest.endSeqno(buf);
        PartitionState ps = channel.getSessionState().get(vbucket);
        ps.useStreamRequest();
        ps.setSnapshotStartSeqno(start);
        ps.setSnapshotEndSeqno(end);
    }

    private void handleOpenStreamResponse(ByteBuf buf) {
        short vbid = (short) MessageUtil.getOpaque(buf);
        PartitionState partitionState = channel.getSessionState().get(vbid);
        OpenStreamResponse response = partitionState.getOpenStreamResponse();
        short status = MessageUtil.getStatus(buf);
        if (LOGGER.isEnabled(CouchbaseLogLevel.DEBUG)) {
            LOGGER.debug("OpenStream {} (0x{}) for vbucket ", MemcachedStatus.toString(status),
                    Integer.toHexString(status), vbid);
        }
        if (status == MemcachedStatus.SUCCESS) {
            if (!channel.openStreams()[vbid]) {
                throw new IllegalStateException("OpenStreamResponse and a request couldn't be found");
            }
            partitionState.setState(PartitionState.CONNECTED);
            updateFailoverLog(buf, vbid);
        } else {
            // Failure
            if (status == MemcachedStatus.ROLLBACK) {
                response.setRollbackSeq(DcpOpenStreamResponse.rollbackSeqno(buf));
            }
            channel.openStreams()[vbid] = false;
            partitionState.setState(PartitionState.DISCONNECTED);
        }
        response.setChannel(channel);
        response.setStatus(status);
        channel.getEnv().eventBus().publish(response);
    }

    private void handleFailoverLogResponse(ByteBuf buf) {
        short vbid = (short) MessageUtil.getOpaque(buf);
        short status = MessageUtil.getStatus(buf);
        if (LOGGER.isEnabled(CouchbaseLogLevel.DEBUG)) {
            LOGGER.debug("FailoverLog {} (0x{}) for vbucket ", MemcachedStatus.toString(status),
                    Integer.toHexString(status), vbid);
        }
        if (status == MemcachedStatus.SUCCESS) {
            handleFailoverLogResponseSuccess(buf, vbid);
        } else {
            if (LOGGER.isEnabled(CouchbaseLogLevel.WARN)) {
                LOGGER.warn("FailoverLog unexpected response: {} (0x{}) for vbucket ", MemcachedStatus.toString(status),
                        Integer.toHexString(status), vbid);
            }
            PartitionState ps = channel.getSessionState().get(vbid);
            ps.failoverRequestFailed(new Exception("Failover response " + MemcachedStatus.toString(status) + "(0x"
                    + Integer.toHexString(status) + ")"));
        }
    }

    private void handleFailoverLogResponseSuccess(ByteBuf buf, short vbid) {
        channel.getFailoverLogRequests()[vbid] = false;
        updateFailoverLog(buf, vbid);
    }

    private void updateFailoverLog(ByteBuf buf, short vbid) {
        ByteBuf failoverLog = Unpooled.buffer();
        DcpFailoverLogResponse.init(failoverLog);
        DcpFailoverLogResponse.vbucket(failoverLog, DcpFailoverLogResponse.vbucket(buf));
        ByteBuf copiedBuf = MessageUtil.getContent(buf).copy().writeShort(vbid);
        MessageUtil.setContent(copiedBuf, failoverLog);
        copiedBuf.release();
        PartitionState ps = channel.getSessionState().get(vbid);
        DcpFailoverLogResponse.fill(failoverLog, ps);
        ps.failoverUpdated();
        channel.getEnv().eventBus().publish(ps.getFailoverLogUpdateEvent());
    }

    private void handleDcpGetPartitionSeqnosResponse(ByteBuf buf) {
        // get status
        short status = MessageUtil.getStatus(buf);
        if (status == MemcachedStatus.SUCCESS) {
            ByteBuf content = MessageUtil.getContent(buf);
            int size = content.readableBytes() / 10;
            for (int i = 0; i < size; i++) {
                int offset = i * 10;
                short vbid = content.getShort(offset);
                long seq = content.getLong(offset + Short.BYTES);
                channel.getSessionState().get(vbid).setCurrentVBucketSeqnoInMaster(seq);
            }
        } else {
            // TODO: find a way to get partitions associated with this node and report failure.
            // Currently, we rely on timeout
        }
        channel.stateFetched();
    }

    private void handleDcpStreamEndMessage(ByteBuf buf) {
        short vbid = DcpStreamEndMessage.vbucket(buf);
        channel.openStreams()[vbid] = false;
        StreamEndReason reason = DcpStreamEndMessage.reason(buf);
        PartitionState state = channel.getSessionState().get(vbid);
        StreamEndEvent endEvent = state.getEndEvent();
        endEvent.setReason(reason);
        if (LOGGER.isEnabled(CouchbaseLogLevel.DEBUG)) {
            LOGGER.debug("Server closed Stream on vbid {} with reason {}", vbid, reason);
        }
        if (channel.getEnv().eventBus() != null) {
            channel.getEnv().eventBus().publish(endEvent);
        }
    }

    private void handleDcpCloseStreamResponse(ByteBuf buf) {
        Short vbid = (short) MessageUtil.getOpaque(buf);
        channel.openStreams()[vbid] = false;
        channel.getSessionState().get(vbid).setState(PartitionState.DISCONNECTED);
        if (LOGGER.isEnabled(CouchbaseLogLevel.DEBUG)) {
            LOGGER.debug("Closed Stream against {} with vbid: {}", channel.getAddress(), vbid);
        }
    }
}

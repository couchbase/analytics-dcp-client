/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DcpAckHandle;
import com.couchbase.client.dcp.events.OpenStreamResponse;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.CollectionsManifest;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.DcpOsoSnapshotMarkerMessage;
import com.couchbase.client.dcp.message.DcpSeqnoAdvancedMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.dcp.message.DcpSystemEvent;
import com.couchbase.client.dcp.message.DcpSystemEventMessage;
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
        switch (buf.getShort(0)) {
            case MessageUtil.STREAM_REQUEST_RESPONSE_PREFIX:
                handleOpenStreamResponse(buf);
                break;
            case MessageUtil.FAILOVER_LOG_RESPONSE_PREFIX:
                handleFailoverLogResponse(buf);
                break;
            case MessageUtil.STREAM_END_REQUEST_PREFIX:
                handleDcpStreamEndMessage(buf);
                break;
            case MessageUtil.STREAM_CLOSE_RESPONSE_PREFIX:
                handleDcpCloseStreamResponse(buf);
                break;
            case MessageUtil.GET_SEQNOS_RESPONSE_PREFIX:
                handleDcpGetPartitionSeqnosResponse(buf);
                break;
            case MessageUtil.SNAPSHOT_MARKER_REQUEST_PREFIX:
                handleDcpSnapshotMarker(buf);
                break;
            case MessageUtil.SET_VBUCKET_STATE_RESPONSE_PREFIX:
                handleDcpStateVbucketStateMessage(buf);
                break;
            case MessageUtil.SEQNO_ADVANCED_REQUEST_PREFIX:
                handleSeqnoAdvanced(buf);
                break;
            case MessageUtil.SYSTEM_EVENT_REQUEST_PREFIX:
                handleSystemEvent(buf);
                break;
            case MessageUtil.OSO_SNAPSHOT_MARKER_REQUEST_PREFIX:
                handleOsoSnapshotMarker(buf);
                break;
            case MessageUtil.GET_COLLECTIONS_MANIFEST_RESPONSE_PREFIX:
                handleCollectionsManifest(buf);
                break;
            default:
                LOGGER.warn("ignoring {}", MessageUtil.humanize(buf));
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
        if (LOGGER.isEnabled(CouchbaseLogLevel.TRACE)) {
            LOGGER.trace("OpenStream {} (0x{}) for vbucket {}", MemcachedStatus.toString(status),
                    Integer.toHexString(status), vbid);
        }
        if (status == MemcachedStatus.SUCCESS) {
            synchronized (channel) {
                if (!channel.openStreams()[vbid]) {
                    if (channel.getSessionState().get(vbid).getState() == PartitionState.DISCONNECTED
                            || channel.getSessionState().get(vbid).getState() == PartitionState.DISCONNECTING) {
                        LOGGER.info("Stream stop was requested before the stream open response is received");
                    } else {
                        throw new IllegalStateException("OpenStreamResponse and a request couldn't be found");
                    }
                } else {
                    partitionState.setState(PartitionState.CONNECTED);
                    updateFailoverLog(buf, vbid);
                }
            }
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
        if (LOGGER.isEnabled(CouchbaseLogLevel.TRACE)) {
            LOGGER.trace("FailoverLog {} (0x{}) for vbucket {}", MemcachedStatus.toString(status),
                    Integer.toHexString(status), vbid);
        }
        if (status == MemcachedStatus.SUCCESS) {
            handleFailoverLogResponseSuccess(buf, vbid);
        } else {
            if (LOGGER.isEnabled(CouchbaseLogLevel.WARN)) {
                LOGGER.warn("FailoverLog unexpected response: {} (0x{}) for vbucket {}",
                        MemcachedStatus.toString(status), Integer.toHexString(status), vbid);
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
        short vbid = (short) MessageUtil.getOpaque(buf);
        channel.openStreams()[vbid] = false;
        channel.getSessionState().get(vbid).setState(PartitionState.DISCONNECTED);
        if (LOGGER.isEnabled(CouchbaseLogLevel.DEBUG)) {
            LOGGER.debug("Closed Stream against {} with vbid: {}", channel.getAddress(), vbid);
        }
    }

    private void handleSeqnoAdvanced(ByteBuf buf) {
        short vbid = MessageUtil.getVbucket(buf);
        long seqno = DcpSeqnoAdvancedMessage.getSeqno(buf);
        LOGGER.debug("Seqno for vbucket {} advanced to {}", vbid, seqno);
        channel.getSessionState().get(vbid).setSeqno(seqno);
    }

    private void handleSystemEvent(ByteBuf buf) {
        short vbid = MessageUtil.getVbucket(buf);
        long seqno = DcpSystemEventMessage.seqno(buf);
        DcpSystemEvent event = DcpSystemEvent.parse(buf);
        PartitionState ps = channel.getSessionState().get(event.getVbucket());
        LOGGER.info("received {}", event);
        if (event.getType() != DcpSystemEvent.Type.UNKNOWN) {
            LOGGER.debug("Seqno for vbucket {} advanced to system event", vbid, seqno);
            ps.setSeqno(seqno);
            ps.setCollectionsManifest(event.apply(ps.getCollectionsManifest()));
        }
    }

    private void handleOsoSnapshotMarker(ByteBuf buf) {
        short vbid = DcpOsoSnapshotMarkerMessage.vbucket(buf);
        boolean begin = DcpOsoSnapshotMarkerMessage.begin(buf);
        boolean end = DcpOsoSnapshotMarkerMessage.end(buf);
        if (!begin && !end) {
            LOGGER.warn("malformed OSO snapshot marker (neither begin nor end): {}",
                    DcpOsoSnapshotMarkerMessage.toString(buf));
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Received {}", DcpOsoSnapshotMarkerMessage.toString(buf));
        }
        if (begin) {
            channel.getSessionState().get(vbid).beginOutOfOrder();
        } else {
            channel.getSessionState().get(vbid).endOutOfOrder();
        }
    }

    private void handleCollectionsManifest(ByteBuf buf) {
        byte[] manifestJsonBytes = MessageUtil.getContentAsByteArray(buf);
        int vbid = MessageUtil.getOpaque(buf);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Got collections manifest: {}", new String(manifestJsonBytes, UTF_8));
        }
        try {
            channel.getSessionState().get(vbid).setCollectionsManifest(CollectionsManifest.fromJson(manifestJsonBytes));
        } catch (IOException e) {
            LOGGER.warn("ignoring malformed manifest update for {}: {}", vbid, new String(manifestJsonBytes, UTF_8), e);
        }
    }
}

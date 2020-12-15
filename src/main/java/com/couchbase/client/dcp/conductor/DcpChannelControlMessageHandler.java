/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import com.couchbase.client.core.CouchbaseException;
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
import com.couchbase.client.dcp.state.SessionPartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.state.StreamState;
import com.couchbase.client.dcp.util.MemcachedStatus;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

import it.unimi.dsi.fastutil.ints.IntSet;

public class DcpChannelControlMessageHandler implements ControlEventHandler {
    private static final CouchbaseLogger LOGGER =
            CouchbaseLoggerFactory.getInstance(DcpChannelControlMessageHandler.class.getName());
    private final DcpChannel channel;

    public DcpChannelControlMessageHandler(DcpChannel channel) {
        this.channel = channel;
    }

    @Override
    public void onEvent(DcpAckHandle ackHandle, ByteBuf buf) {
        switch (buf.getByte(1)) {
            case MessageUtil.DCP_STREAM_REQUEST_OPCODE:
                handleOpenStreamResponse(buf);
                break;
            case MessageUtil.DCP_FAILOVER_LOG_OPCODE:
                handleFailoverLogResponse(buf);
                break;
            case MessageUtil.DCP_STREAM_END_OPCODE:
                handleDcpStreamEndMessage(buf);
                break;
            case MessageUtil.DCP_STREAM_CLOSE_OPCODE:
                handleDcpCloseStreamResponse(buf);
                break;
            case MessageUtil.GET_ALL_VB_SEQNOS_OPCODE:
                handleDcpGetPartitionSeqnosResponse(buf);
                break;
            case MessageUtil.DCP_SNAPSHOT_MARKER_OPCODE:
                handleDcpSnapshotMarker(buf);
                break;
            case MessageUtil.DCP_SET_VBUCKET_STATE_OPCODE:
                handleDcpStateVbucketStateMessage(buf);
                break;
            case MessageUtil.DCP_SEQNO_ADVANCED_OPCODE:
                handleSeqnoAdvanced(buf);
                break;
            case MessageUtil.DCP_SYSTEM_EVENT_OPCODE:
                handleSystemEvent(buf);
                break;
            case MessageUtil.DCP_OSO_SNAPSHOT_MARKER_OPCODE:
                handleOsoSnapshotMarker(buf);
                break;
            case MessageUtil.DCP_COLLECTIONS_MANIFEST_OPCODE:
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
        StreamPartitionState ps = MessageUtil.streamState(buf, channel).get(vbucket);
        ps.useStreamRequest();
        ps.setSnapshotStartSeqno(start);
        ps.setSnapshotEndSeqno(end);
    }

    private void handleOpenStreamResponse(ByteBuf buf) {
        short vbid = DcpOpenStreamResponse.vbucket(buf);
        int streamId = DcpOpenStreamResponse.streamId(buf);
        final StreamState ss = channel.getSessionState().streamState(streamId);
        StreamPartitionState partitionState = ss.get(vbid);
        OpenStreamResponse response = partitionState.getOpenStreamResponse();
        short status = MessageUtil.getStatus(buf);
        if (LOGGER.isEnabled(CouchbaseLogLevel.TRACE)) {
            LOGGER.trace("OpenStream {} (0x{}) for vbucket {} on stream {}", MemcachedStatus.toString(status),
                    Integer.toHexString(status), vbid, ss.streamId());
        }
        if (status == MemcachedStatus.SUCCESS) {
            synchronized (channel) {
                if (channel.openStreams()[vbid] == null || !channel.openStreams()[vbid].contains(ss.streamId())) {
                    if (ss.get(vbid).getState() == StreamPartitionState.DISCONNECTED
                            || ss.get(vbid).getState() == StreamPartitionState.DISCONNECTING) {
                        LOGGER.info("Stream stop was requested before the stream open response is received");
                    } else {
                        throw new IllegalStateException("OpenStreamResponse and a request couldn't be found");
                    }
                } else {
                    partitionState.setState(StreamPartitionState.CONNECTED);
                    updateFailoverLog(buf, vbid);
                }
            }
        } else {
            // Failure
            if (status == MemcachedStatus.ROLLBACK) {
                response.setRollbackSeq(DcpOpenStreamResponse.rollbackSeqno(buf));
            }
            clearOpen(ss, vbid);
            partitionState.setState(StreamPartitionState.DISCONNECTED);
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
            channel.getSessionState().get(vbid).failoverLogsRequestFailed(new Exception("Failover response "
                    + MemcachedStatus.toString(status) + "(0x" + Integer.toHexString(status) + ")"));
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
        final SessionState ss = channel.getSessionState();
        final SessionPartitionState ps = ss.get(vbid);
        DcpFailoverLogResponse.fill(failoverLog, ps);
        ps.failoverUpdated();
        channel.getEnv().eventBus().publish(ps.getFailoverLogUpdateEvent());
    }

    private void handleDcpGetPartitionSeqnosResponse(ByteBuf buf) {
        // get status
        short status = MessageUtil.getStatus(buf);
        int streamId = MessageUtil.getOpaque(buf);

        if (status == MemcachedStatus.SUCCESS) {
            ByteBuf content = MessageUtil.getContent(buf);
            int size = content.readableBytes();
            for (int offset = 0; offset < size; offset += 10) {
                short vbid = content.getShort(offset);
                long seq = content.getLong(offset + Short.BYTES);
                channel.getSessionState().streamState(streamId).setCurrentVBucketSeqnoInMaster(vbid, seq);
            }
        } else {
            channel.getSessionState().streamState(streamId)
                    .seqsRequestFailed(new CouchbaseException(MemcachedStatus.toString(status)));
        }
        channel.stateFetched(streamId);
    }

    private void handleDcpStreamEndMessage(ByteBuf buf) {
        short vbid = DcpStreamEndMessage.vbucket(buf);
        clearOpen(MessageUtil.streamState(buf, channel), vbid);
        StreamEndReason reason = DcpStreamEndMessage.reason(buf);
        StreamPartitionState state = MessageUtil.streamState(buf, channel).get(vbid);
        StreamEndEvent endEvent = state.getEndEvent();
        endEvent.setReason(reason);
        if (LOGGER.isEnabled(CouchbaseLogLevel.DEBUG)) {
            LOGGER.debug("Server closed Stream on vbid {} with reason {}", vbid, reason);
        }
        if (channel.getEnv().eventBus() != null) {
            channel.getEnv().eventBus().publish(endEvent);
        }
    }

    private void clearOpen(StreamState ss, short vbid) {
        final IntSet openStreams = channel.openStreams()[vbid];
        if (openStreams != null) {
            openStreams.remove(ss.streamId());
        }
    }

    private void handleDcpCloseStreamResponse(ByteBuf buf) {
        short vbid = (short) MessageUtil.getOpaque(buf);
        clearOpen(MessageUtil.streamState(buf, channel), vbid);
        MessageUtil.streamState(buf, channel).get(vbid).setState(StreamPartitionState.DISCONNECTED);
        if (LOGGER.isEnabled(CouchbaseLogLevel.DEBUG)) {
            LOGGER.debug("Closed Stream against {} with vbid: {}", channel.getAddress(), vbid);
        }
    }

    private void handleSeqnoAdvanced(ByteBuf buf) {
        short vbid = MessageUtil.getVbucket(buf);
        long seqno = DcpSeqnoAdvancedMessage.getSeqno(buf);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Seqno for vbucket {} advanced to {}", vbid, seqno);
        }
        MessageUtil.streamState(buf, channel).get(vbid).advanceSeqno(seqno);
    }

    private void handleSystemEvent(ByteBuf buf) {
        short vbid = MessageUtil.getVbucket(buf);
        long seqno = DcpSystemEventMessage.seqno(buf);
        DcpSystemEvent event = DcpSystemEvent.parse(buf);
        StreamPartitionState ps = MessageUtil.streamState(buf, channel).get(event.getVbucket());
        LOGGER.trace("received {}", event);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Seqno for vbucket {} advanced to {} on system event", vbid, seqno);
        }
        ps.onSystemEvent(event);
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
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Received {}", DcpOsoSnapshotMarkerMessage.toString(buf));
        }
        if (begin) {
            MessageUtil.streamState(buf, channel).get(vbid).beginOutOfOrder();
        } else {
            long maxSeqNo = MessageUtil.streamState(buf, channel).get(vbid).endOutOfOrder();
            // we store the max sequence number in the message as it may be needed by other event
            DcpOsoSnapshotMarkerMessage.setMaxSeqNo(maxSeqNo, buf);
        }
    }

    private void handleCollectionsManifest(ByteBuf buf) {
        byte[] manifestJsonBytes = MessageUtil.getContentAsByteArray(buf);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Got collections manifest: {}", new String(manifestJsonBytes, UTF_8));
        }
        try {
            channel.getSessionState().onCollectionsManifest(CollectionsManifest.fromJson(manifestJsonBytes));
        } catch (IOException e) {
            LOGGER.error("malformed collections manifest {}", new String(manifestJsonBytes, UTF_8), e);
            channel.getSessionState().onCollectionsManifestFailure(e);
        }
    }
}

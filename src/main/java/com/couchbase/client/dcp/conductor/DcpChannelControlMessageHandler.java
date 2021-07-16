/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.conductor;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DcpAckHandle;
import com.couchbase.client.dcp.events.OpenStreamResponse;
import com.couchbase.client.dcp.events.OpenStreamRollbackResponse;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.CollectionsManifest;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosResponse;
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
import com.couchbase.client.dcp.transport.netty.DcpMessageHandler;
import com.couchbase.client.dcp.transport.netty.Stat;
import com.couchbase.client.dcp.util.CollectionsUtil;
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
        try {
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
                case MessageUtil.STAT_OPCODE:
                    handleStatResponse(buf);
                    break;
                default:
                    LOGGER.warn("ignoring {}", MessageUtil.humanize(buf));
            }
        } catch (Exception e) {
            LOGGER.error(e);
            channel.getEnv().controlEventHandler().onEvent(ackHandle, buf);
            throw e;
        } catch (Throwable t) {
            LOGGER.error(t);
            DcpMessageHandler.ackAndRelease(ackHandle, buf);
            throw t;
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
        short status = MessageUtil.getStatus(buf);
        OpenStreamResponse response;
        if (LOGGER.isEnabled(CouchbaseLogLevel.TRACE)) {
            LOGGER.trace("OpenStream {} (0x{}) for vbucket {} on stream {}", MemcachedStatus.toString(status),
                    Integer.toHexString(status), vbid, ss.streamId());
        }
        if (status == MemcachedStatus.SUCCESS) {
            response = new OpenStreamResponse(partitionState, status);
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
                response = new OpenStreamRollbackResponse(partitionState, DcpOpenStreamResponse.rollbackSeqno(buf));
            } else {
                response = new OpenStreamResponse(partitionState, status);
            }
            clearOpen(ss, vbid);
            partitionState.setState(StreamPartitionState.DISCONNECTED);
        }
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
        short status = DcpGetPartitionSeqnosResponse.getStatus(buf);
        int cid = DcpGetPartitionSeqnosResponse.getCid(buf);

        final SessionState sessionState = channel.getSessionState();
        if (status == MemcachedStatus.SUCCESS) {
            sessionState.handleSeqnoResponse(cid, MessageUtil.getContent(buf));
        } else {
            sessionState.seqnoRequestFailed(cid, new CouchbaseException(MemcachedStatus.toString(status)));
        }
        channel.seqnosFetched(cid);
    }

    private void handleDcpStreamEndMessage(ByteBuf buf) {
        short vbid = DcpStreamEndMessage.vbucket(buf);
        final StreamState streamState = MessageUtil.streamState(buf, channel);
        clearOpen(streamState, vbid);
        StreamEndReason reason = DcpStreamEndMessage.reason(buf);
        StreamPartitionState state = streamState.get(vbid);
        StreamEndEvent endEvent = new StreamEndEvent(state, streamState, reason);
        if (LOGGER.isEnabled(CouchbaseLogLevel.DEBUG)) {
            LOGGER.debug("Server closed Stream on vbid {} with reason {}", vbid, reason);
        }
        if (channel.getEnv().eventBus() != null) {
            channel.getEnv().eventBus().publish(endEvent);
        }
    }

    private void clearOpen(StreamState ss, short vbid) {
        synchronized (channel) {
            final IntSet openStreams = channel.openStreams()[vbid];
            if (openStreams != null) {
                openStreams.remove(ss.streamId());
            }
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

    private void handleStatResponse(ByteBuf buf) {
        int kindId = MessageUtil.getOpaqueLo(buf);
        Stat.Kind statKind = Stat.Kind.valueOf(kindId, Stat.Kind.UNKNOWN);
        String key = MessageUtil.getKeyAsString(buf, false);
        if (key.isEmpty()) {
            LOGGER.trace("handleStatResponse: {} <end>", statKind);
            return;
        }
        switch (statKind) {
            case COLLECTIONS_BYID:
                String value = MessageUtil.getContentAsString(buf);
                LOGGER.trace("handleStatResponse: {} {}={}", statKind, key, value);
                String[] parts = StringUtils.split(key, ':');
                switch (Stat.CollectionsByid.parseStatParts(parts)) {
                    case ITEMS:
                        // 0x8:0x8:items=4999
                        int cid = CollectionsUtil.decodeCid(parts[1].substring(2));
                        long count = Long.parseLong(value);
                        channel.getSessionState().recordItemCountResponse(cid, count);
                        break;
                    default:
                        // ignoring unknown stat
                }
                break;
            default:
                LOGGER.warn("unrecognized stat response {}", MessageUtil.humanize(buf));
                throw new IllegalArgumentException("unrecognized stat response: kind=" + kindId);
        }
    }
}

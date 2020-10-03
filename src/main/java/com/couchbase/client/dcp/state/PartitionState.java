/*
 * Copyright (c) 2016 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.events.FailoverLogUpdateEvent;
import com.couchbase.client.dcp.events.OpenStreamResponse;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.CollectionsManifest;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Represents the individual current session state for a given partition.
 */
public class PartitionState {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final long INVALID = -1L;
    public static final byte DISCONNECTED = 0x00;
    public static final byte CONNECTING = 0x02;
    public static final byte CONNECTED = 0x03;
    public static final byte DISCONNECTING = 0x04;
    public static final long RECOVERING = 0x05;
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Stores the failover log for this partition.
     */
    private final List<FailoverLogEntry> failoverLog;
    private volatile long currentVBucketSeqnoInMaster = INVALID;

    private final short vbid;

    /**
     * Current Sequence Number
     */
    private volatile long seqno = 0;

    private volatile long streamEndSeq = 0;

    private volatile long uuid = 0;

    private volatile long snapshotStartSeqno = 0;

    private volatile long snapshotEndSeqno = 0;

    private volatile byte state;

    private volatile boolean failoverUpdated;

    private volatile boolean currentSeqUpdated;

    private volatile boolean collectionsManifestUpdated;

    private volatile boolean clientDisconnected;

    private volatile Throwable failoverLogRequestFailure;

    private volatile Throwable seqsRequestFailure;

    private volatile Throwable collectionsManifestFailure;

    private volatile boolean outOfOrder;

    private StreamRequest streamRequest;

    private final StreamEndEvent endEvent;
    private final FailoverLogUpdateEvent failoverLogUpdateEvent;

    private final OpenStreamResponse openStreamResponse;

    private volatile CollectionsManifest collectionsManifest;

    /**
     * Initialize a new partition state.
     */
    public PartitionState(short vbid) {
        this.vbid = vbid;
        failoverLog = new ArrayList<>();
        setState(DISCONNECTED);
        failoverUpdated = false;
        currentSeqUpdated = false;
        endEvent = new StreamEndEvent(this);
        failoverLogUpdateEvent = new FailoverLogUpdateEvent(this);
        openStreamResponse = new OpenStreamResponse(this);
    }

    public long getSnapshotStartSeqno() {
        return snapshotStartSeqno;
    }

    public void setSnapshotStartSeqno(long snapshotStartSeqno) {
        this.snapshotStartSeqno = snapshotStartSeqno;
    }

    public long getSnapshotEndSeqno() {
        return snapshotEndSeqno;
    }

    public void setSnapshotEndSeqno(long snapshotEndSeqno) {
        this.snapshotEndSeqno = snapshotEndSeqno;
        currentVBucketSeqnoInMaster = Long.max(currentVBucketSeqnoInMaster, snapshotEndSeqno);
    }

    /**
     * Returns the full failover log stored, in sorted order.
     * index of more recent history entry > index of less recent history entry
     */
    public List<FailoverLogEntry> getFailoverLog() {
        return failoverLog;
    }

    public boolean hasFailoverLogs() {
        return !failoverLog.isEmpty();
    }

    /**
     * Add a new seqno/uuid combination to the failover log.
     *
     * @param seqno
     *            the sequence number.
     * @param vbuuid
     *            the uuid for the sequence.
     */
    public void addToFailoverLog(long seqno, long vbuuid) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.log(Level.TRACE, "Adding failover log entry: (" + vbuuid + "-" + seqno + ") for vbucket " + vbid);
        }
        failoverLog.add(new FailoverLogEntry(seqno, vbuuid));
        uuid = vbuuid;
    }

    /**
     * Returns the current sequence number.
     */
    public long getSeqno() {
        return seqno;
    }

    /**
     * Allows to set the current sequence number.
     */
    public void setSeqno(long seqno) {
        if (!outOfOrder && Long.compareUnsigned(seqno, this.seqno) <= 0) {
            LOGGER.warn("new seqno received (0x{}) <= the previous seqno(0x{}) for vbid: {}",
                    Long.toUnsignedString(seqno, 16), Long.toUnsignedString(this.seqno, 16), vbid);
        }
        this.seqno = seqno;
    }

    public byte getState() {
        return state;
    }

    public synchronized void setState(byte state) {
        this.state = state;
        notifyAll();
    }

    public synchronized void wait(byte state) throws InterruptedException {
        LOGGER.trace("Waiting until state is {} for {}", state, vbid);
        while (this.state != state) {
            wait();
        }
    }

    public void setStreamEndSeq(long seq) {
        this.streamEndSeq = seq;
    }

    public long getStreamEndSeq() {
        return streamEndSeq;
    }

    public StreamRequest getStreamRequest() {
        return streamRequest;
    }

    public void setStreamRequest(StreamRequest streamRequest) {
        this.streamRequest = streamRequest;
        seqno = streamRequest.getStartSeqno();
        streamEndSeq = streamRequest.getEndSeqno();
        uuid = streamRequest.getVbucketUuid();
        snapshotStartSeqno = streamRequest.getSnapshotStartSeqno();
        snapshotEndSeqno = streamRequest.getSnapshotEndSeqno();
    }

    public void prepareNextStreamRequest() {
        if (streamRequest == null) {
            if (snapshotStartSeqno > seqno) {
                snapshotStartSeqno = seqno;
            }
            if (SessionState.NO_END_SEQNO != streamEndSeq && Long.compareUnsigned(streamEndSeq, seqno) < 0) {
                streamEndSeq = snapshotEndSeqno;
            }
            this.streamRequest =
                    new StreamRequest(vbid, seqno, streamEndSeq, uuid, snapshotStartSeqno, snapshotEndSeqno);
        }
    }

    public short vbid() {
        return vbid;
    }

    public long getCurrentVBucketSeqnoInMaster() {
        return currentVBucketSeqnoInMaster;
    }

    public synchronized void setCurrentVBucketSeqnoInMaster(long currentVBucketSeqnoInMaster) {
        this.currentVBucketSeqnoInMaster = currentVBucketSeqnoInMaster;
        currentSeqUpdated = true;
        notifyAll();
    }

    public void useStreamRequest() {
        streamRequest = null;
    }

    @Override
    public String toString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(toMap());
        } catch (IOException e) {
            LOGGER.log(Level.WARN, e);
            return "{\"" + this.getClass().getSimpleName() + "\":\"" + e.toString() + "\"}";
        }
    }

    public Map<String, Object> toMap() {
        Map<String, Object> tree = new HashMap<>();
        tree.put("vbid", vbid);
        tree.put("maxSeq", currentVBucketSeqnoInMaster);
        tree.put("uuid", uuid);
        tree.put("seqno", seqno);
        tree.put("state", state);
        tree.put("failoverLog", failoverLog);
        return tree;
    }

    public synchronized void failoverUpdated() {
        LOGGER.trace("Failover log updated for {}", vbid);
        failoverUpdated = true;
        notifyAll();
    }

    public void failoverRequest() {
        LOGGER.trace("Failover log requested for {}", vbid);
        failoverUpdated = false;
        failoverLogRequestFailure = null;
    }

    public void currentSeqRequest() {
        currentSeqUpdated = false;
        seqsRequestFailure = null;
    }

    public void collectionsManifestRequest() {
        LOGGER.trace("Collections manifest requested for {}", vbid);
        collectionsManifestUpdated = false;
        collectionsManifestFailure = null;
    }

    public synchronized void waitTillFailoverUpdated(long timeout) throws Throwable {
        Span span = Span.start(timeout, TimeUnit.MILLISECONDS);
        LOGGER.trace("Waiting until failover log updated for {}", vbid);
        while (!clientDisconnected && failoverLogRequestFailure == null && !failoverUpdated && !span.elapsed()) {
            span.wait(this);
        }
        if (clientDisconnected) {
            throw new CancellationException("Client disconnected while waiting for reply");
        }
        if (failoverLogRequestFailure != null) {
            throw failoverLogRequestFailure;
        }
        if (!failoverUpdated) {
            throw new TimeoutException(timeout / 1000.0 + "s passed before obtaining failover logs for " + vbid);
        }
    }

    public synchronized void waitTillCurrentSeqUpdated(long timeout) throws Throwable {
        Span span = Span.start(timeout, TimeUnit.MILLISECONDS);
        LOGGER.trace("Waiting until current seq updated for {}", vbid);
        while (!clientDisconnected && seqsRequestFailure == null && !currentSeqUpdated && !span.elapsed()) {
            span.wait(this);
        }
        if (clientDisconnected) {
            throw new CancellationException("Client disconnected while waiting for reply");
        }
        if (seqsRequestFailure != null) {
            throw failoverLogRequestFailure;
        }
        if (!currentSeqUpdated) {
            throw new TimeoutException(timeout / 1000.0 + "s passed before obtaining current seq for " + vbid);
        }
    }

    public synchronized void waitCollectionsManifestUpdated(long timeout) throws Throwable {
        Span span = Span.start(timeout, TimeUnit.MILLISECONDS);
        LOGGER.trace("Waiting until failover log updated for {}", vbid);
        while (!clientDisconnected && collectionsManifestFailure == null && !collectionsManifestUpdated
                && !span.elapsed()) {
            span.wait(this);
        }
        if (clientDisconnected) {
            throw new CancellationException("Client disconnected while waiting for reply");
        }
        if (collectionsManifestFailure != null) {
            throw collectionsManifestFailure;
        }
        if (!collectionsManifestUpdated) {
            throw new TimeoutException(timeout / 1000.0 + "s passed before obtaining collections manifest for " + vbid);
        }
    }

    public StreamEndEvent getEndEvent() {
        return endEvent;
    }

    public FailoverLogUpdateEvent getFailoverLogUpdateEvent() {
        return failoverLogUpdateEvent;
    }

    public OpenStreamResponse getOpenStreamResponse() {
        return openStreamResponse;
    }

    public long getUuid() {
        return uuid;
    }

    public int getFailoverLogSize() {
        return failoverLog.size();
    }

    public FailoverLogEntry getFailoverLog(int i) {
        return failoverLog.get(i);
    }

    public synchronized void failoverRequestFailed(Throwable th) {
        failoverLogRequestFailure = th;
        notifyAll();
    }

    public synchronized void seqsRequestFailed(Throwable th) {
        seqsRequestFailure = th;
        notifyAll();
    }

    public synchronized void collectionsManifestRequestFailed(Throwable th) {
        collectionsManifestFailure = th;
        notifyAll();
    }

    public synchronized void clientDisconnected() {
        clientDisconnected = true;
        notifyAll();
    }

    public synchronized void clientConnected() {
        clientDisconnected = false;
        notifyAll();
    }

    public boolean isClientDisconnected() {
        return clientDisconnected;
    }

    public void clearFailoverLog() {
        failoverLog.clear();
    }

    public void beginOutOfOrder() {
        outOfOrder = true;
    }

    public void endOutOfOrder() {
        outOfOrder = false;
    }

    public CollectionsManifest getCollectionsManifest() {
        return collectionsManifest;
    }

    public synchronized void setCollectionsManifest(CollectionsManifest collectionsManifest) {
        this.collectionsManifest = requireNonNull(collectionsManifest);
        collectionsManifestUpdated = true;
        notifyAll();
    }

}

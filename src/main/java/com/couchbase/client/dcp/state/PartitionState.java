/*
 * Copyright (c) 2016 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

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
import com.couchbase.client.dcp.events.PartitionUUIDChangeEvent;
import com.couchbase.client.dcp.events.StreamEndEvent;
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

    private volatile long uuid = 0;

    private volatile long snapshotStartSeqno = 0;

    private volatile long snapshotEndSeqno = 0;

    private volatile byte state;

    private volatile boolean failoverUpdated;

    private volatile boolean currentSeqUpdated;

    private volatile boolean clientDisconnected;

    private volatile Throwable failoverLogRequestFailure;

    private volatile Throwable seqsRequestFailure;

    private long nextUuidChangeSeq;

    private StreamRequest streamRequest;

    private final StreamEndEvent endEvent;
    private final FailoverLogUpdateEvent failoverLogUpdateEvent;
    private final PartitionUUIDChangeEvent uuidChangeEvent;

    private final OpenStreamResponse openStreamResponse;

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
        uuidChangeEvent = new PartitionUUIDChangeEvent(this);
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.log(Level.DEBUG, "Adding failover log entry: (" + vbuuid + "-" + seqno + ")");
        }
        failoverLog.add(new FailoverLogEntry(seqno, vbuuid));
        resetCurrentUUid();
    }

    private void resetCurrentUUid() {
        long theUuid = 0L;
        for (int i = 0; i < failoverLog.size(); i++) {
            FailoverLogEntry next = failoverLog.get(i);
            if (next.getSeqno() >= seqno) {
                break;
            }
            theUuid = next.getUuid();
        }
        uuid = theUuid;
        nextUuidChangeSeq = getNextUuidChangeSeq();
    }

    private long getNextUuidChangeSeq() {
        if (uuid == 0) {
            return 0L;
        }
        int i = 0;
        for (; i < failoverLog.size(); i++) {
            FailoverLogEntry next = failoverLog.get(i);
            if (next.getUuid() == uuid) {
                break;
            }
        }
        i++;
        return i < failoverLog.size() ? failoverLog.get(i).getSeqno() : Long.MAX_VALUE;
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
    public boolean hasBucketUuidChanged(long seqno) {
        if (seqno <= this.seqno) {
            LOGGER.warn("A bug. sequence number received(" + seqno + ") <= the previous sequence number(" + this.seqno
                    + ")");
        }
        this.seqno = seqno;
        if (seqno > nextUuidChangeSeq) {
            resetCurrentUUid();
            return true;
        }
        return false;

    }

    public byte getState() {
        return state;
    }

    public synchronized void setState(byte state) {
        this.state = state;
        notifyAll();
    }

    public synchronized void wait(byte state) throws InterruptedException {
        LOGGER.debug("Waiting until state is " + state);
        while (this.state != state) {
            wait();
        }
    }

    public StreamRequest getStreamRequest() {
        return streamRequest;
    }

    public void setStreamRequest(StreamRequest streamRequest) {
        this.streamRequest = streamRequest;
        seqno = streamRequest.getStartSeqno();
        uuid = streamRequest.getVbucketUuid();
    }

    public void prepareNextStreamRequest() {
        if (streamRequest == null) {
            this.streamRequest = new StreamRequest(vbid, seqno, SessionState.NO_END_SEQNO, uuid, snapshotStartSeqno,
                    snapshotEndSeqno);
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
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(toMap());
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
        LOGGER.debug("Failover log updated");
        failoverUpdated = true;
        notifyAll();
    }

    public void failoverRequest() {
        LOGGER.debug("Failover log requested");
        failoverUpdated = false;
        failoverLogRequestFailure = null;
    }

    public void currentSeqRequest() {
        LOGGER.debug("Current Seq requested");
        currentSeqUpdated = false;
        seqsRequestFailure = null;
    }

    public synchronized void waitTillFailoverUpdated(long timeout) throws Throwable {
        Span span = Span.start(timeout, TimeUnit.MILLISECONDS);
        LOGGER.debug("Waiting until failover log updated");
        while (!clientDisconnected && failoverLogRequestFailure == null && !failoverUpdated && !span.elapsed()) {
            TimeUnit.NANOSECONDS.timedWait(this, span.remaining(TimeUnit.NANOSECONDS));
        }
        if (clientDisconnected) {
            throw new CancellationException("Client disconnected while waiting for reply");
        }
        if (failoverLogRequestFailure != null) {
            throw failoverLogRequestFailure;
        }
        if (!failoverUpdated) {
            throw new TimeoutException(timeout / 1000.0 + "s passed before obtaining failover logs for this partition");
        }
    }

    public synchronized void waitTillCurrentSeqUpdated(long timeout) throws Throwable {
        Span span = Span.start(timeout, TimeUnit.MILLISECONDS);
        LOGGER.debug("Waiting until failover log updated");
        while (!clientDisconnected && seqsRequestFailure == null && !currentSeqUpdated && !span.elapsed()) {
            TimeUnit.NANOSECONDS.timedWait(this, span.remaining(TimeUnit.NANOSECONDS));
        }
        if (clientDisconnected) {
            throw new CancellationException("Client disconnected while waiting for reply");
        }
        if (seqsRequestFailure != null) {
            throw failoverLogRequestFailure;
        }
        if (!currentSeqUpdated) {
            throw new TimeoutException(timeout / 1000.0 + "s passed before obtaining failover logs for this partition");
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

    public PartitionUUIDChangeEvent getUuidChangeEvent() {
        return uuidChangeEvent;
    }
}

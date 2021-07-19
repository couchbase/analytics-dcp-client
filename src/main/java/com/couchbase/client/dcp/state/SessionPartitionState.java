/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.state;

import static com.couchbase.client.dcp.state.StreamPartitionState.INVALID_SEQNO;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.events.DcpEvent;
import com.couchbase.client.dcp.events.FailoverLogUpdateEvent;

public class SessionPartitionState {
    private static final Logger LOGGER = LogManager.getLogger();
    private final short vbid;

    private final List<FailoverLogEntry> failoverLog = new ArrayList<>();

    private long uuid;

    private volatile boolean failoverUpdated;

    private volatile Throwable failoverLogRequestFailure;

    private final FailoverLogUpdateEvent failoverLogUpdateEvent;

    private long currentVBucketSeqnoInMaster = INVALID_SEQNO;

    private Throwable seqsRequestFailure;

    public SessionPartitionState(short vbid) {
        this.vbid = vbid;
        failoverUpdated = false;
        failoverLogUpdateEvent = new FailoverLogUpdateEvent(this);
    }

    public void setSnapshotEndSeqno(long snapshotEndSeqno) {
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

    public int getFailoverLogSize() {
        return failoverLog.size();
    }

    public FailoverLogEntry getFailoverLog(int i) {
        return failoverLog.get(i);
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

    public void clearFailoverLog() {
        failoverLog.clear();
    }

    public synchronized void waitTillFailoverUpdated(SessionState sessionState, long timeout, TimeUnit timeUnit)
            throws Throwable {
        Span span = Span.start(timeout, timeUnit);
        LOGGER.trace("Waiting until failover log updated for {}", vbid);
        while (sessionState.isConnected() && failoverLogRequestFailure == null && !failoverUpdated && !span.elapsed()) {
            span.wait(this);
        }
        if (!sessionState.isConnected()) {
            throw new CancellationException("Client disconnected while waiting for reply");
        }
        if (failoverLogRequestFailure != null) {
            throw failoverLogRequestFailure;
        }
        if (!failoverUpdated) {
            throw new TimeoutException(timeout / 1000.0 + "s passed before obtaining failover logs for " + vbid);
        }
    }

    public DcpEvent getFailoverLogUpdateEvent() {
        return failoverLogUpdateEvent;
    }

    public synchronized void failoverLogsRequestFailed(Throwable t) {
        failoverLogRequestFailure = t;
        notifyAll();
    }

    public short vbid() {
        return vbid;
    }

    public long getCurrentVBucketSeqnoInMaster() {
        return currentVBucketSeqnoInMaster;
    }

    public void setCurrentVBucketSeqnoInMaster(long currentVBucketSeqnoInMaster) {
        this.currentVBucketSeqnoInMaster = currentVBucketSeqnoInMaster;
    }

    public long uuid() {
        return uuid;
    }
}

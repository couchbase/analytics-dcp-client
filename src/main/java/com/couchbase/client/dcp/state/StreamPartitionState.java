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

import static com.couchbase.client.dcp.util.MathUtil.maxUnsigned;
import static org.apache.hyracks.util.Span.ELAPSED;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.util.Span;
import org.apache.hyracks.util.annotations.GuardedBy;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.message.DcpDataMessage;
import com.couchbase.client.dcp.message.DcpSystemEvent;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.MemcachedStatus;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

/**
 * Represents the individual current session state for a given partition.
 */
public class StreamPartitionState {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final long INVALID_SEQNO = -1L;
    public static final byte DISCONNECTED = 0x00;
    public static final byte CONNECTING = 0x02;
    public static final byte CONNECTED = 0x03;
    public static final byte DISCONNECTING = 0x04;
    public static final byte CONNECTED_OSO = 0x05;
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private volatile long currentVBucketSeqnoInMaster = INVALID_SEQNO;

    private final short vbid;

    /**
     * Current Sequence Number
     */
    private volatile long seqno = INVALID_SEQNO;

    private volatile long streamEndSeq = 0;

    private volatile long snapshotStartSeqno = 0;

    private volatile long snapshotEndSeqno = 0;

    private volatile byte state;

    private volatile long osoMaxSeqno = 0;

    private volatile long seqnoAdvances = 0;

    private volatile long mutationsProcessed = 0;

    private volatile long deletionsProcessed = 0;

    private StreamRequest streamRequest;

    private long manifestUid;

    private Span delay = ELAPSED;

    /**
     * Initialize a new partition state.
     */
    public StreamPartitionState(short vbid) {
        this.vbid = vbid;
        state = DISCONNECTED;
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

    @GuardedBy("operations on a vbucket do not interleave")
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void setSnapshotEndSeqno(long snapshotEndSeqno) {
        this.snapshotEndSeqno = snapshotEndSeqno;
        currentVBucketSeqnoInMaster = maxUnsigned(currentVBucketSeqnoInMaster, snapshotEndSeqno);
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
    @GuardedBy("operations on a vbucket do not interleave")
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void setSeqno(long seqno) {
        if (state == CONNECTED_OSO) {
            osoMaxSeqno = maxUnsigned(seqno, osoMaxSeqno);
        } else {
            if (Long.compareUnsigned(seqno, this.seqno) <= 0 && this.seqno != INVALID_SEQNO) {
                LOGGER.warn("new seqno received (0x{}) <= the previous seqno(0x{}) for vbid: {}",
                        Long.toUnsignedString(seqno, 16), Long.toUnsignedString(this.seqno, 16), vbid);
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("setting seqno to {} for vbid {} on setSeqno", seqno, vbid);
            }
            seqnoAdvances += this.seqno != INVALID_SEQNO ? (seqno - this.seqno) : seqno;
            this.seqno = seqno;
        }
    }

    /**
     * Allows to set the current sequence number.
     */
    public void advanceSeqno(long seqno) {
        setSeqno(seqno);
        setSnapshotStartSeqno(seqno);
        setSnapshotEndSeqno(seqno);
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
        LOGGER.trace("setting seqno to {} for vbid {} on setStreamRequest", seqno, vbid);
        seqno = streamRequest.getStartSeqno();
        streamEndSeq = streamRequest.getEndSeqno();
        snapshotStartSeqno = streamRequest.getSnapshotStartSeqno();
        snapshotEndSeqno = streamRequest.getSnapshotEndSeqno();
        manifestUid = streamRequest.getManifestUid();
    }

    public void prepareNextStreamRequest(SessionState sessionState, StreamState streamState) {
        if (streamRequest == null) {
            if (snapshotStartSeqno > seqno) {
                snapshotStartSeqno = seqno;
            }
            if (SessionState.NO_END_SEQNO != streamEndSeq && Long.compareUnsigned(streamEndSeq, seqno) < 0) {
                streamEndSeq = snapshotEndSeqno;
            }
            this.streamRequest =
                    new StreamRequest(vbid, seqno, streamEndSeq, sessionState.get(vbid).uuid(), snapshotStartSeqno,
                            snapshotEndSeqno, manifestUid, streamState.streamId(), streamState.collectionId());
        }
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
        tree.put("seqno", seqno);
        tree.put("state", state);
        tree.put("osoMaxSeq", osoMaxSeqno);
        return tree;
    }

    public void beginOutOfOrder() {
        state = CONNECTED_OSO;
        osoMaxSeqno = seqno;
    }

    /**
     * @return the new seqno due to the completed OSO snapshot, or {@link StreamPartitionState#INVALID_SEQNO} (i.e. -1)
     *         if the OSO snapshot was empty
     */
    public long endOutOfOrder() {
        // On disconnect after successfully receiving the OSO end, reconnect
        // with a stream-request where start=X, snap.start=X, snap.end=X
        useStreamRequest();
        state = CONNECTED;
        boolean noop = osoMaxSeqno == seqno;
        if (!noop) {
            setSeqno(osoMaxSeqno);
        }
        setSnapshotStartSeqno(osoMaxSeqno);
        setSnapshotEndSeqno(osoMaxSeqno);
        return noop ? INVALID_SEQNO : osoMaxSeqno;
    }

    public boolean isOsoSnapshot() {
        return state == CONNECTED_OSO;
    }

    public long getSeqnoAdvances() {
        return seqnoAdvances;
    }

    public void onSystemEvent(DcpSystemEvent event) {
        setSeqno(event.getSeqno());
        manifestUid = event.getManifestUid();
    }

    public void calculateNextDelay(short status) {
        if (status == MemcachedStatus.SUCCESS) {
            delay = ELAPSED;
        } else {
            if (delay.getSpanNanos() == 0) {
                // start with 1s
                delay = Span.start(1, TimeUnit.SECONDS);
            } else {
                // double the delay, capping at 64s
                delay = Span.start(Long.min(delay.getSpanNanos() * 2, TimeUnit.SECONDS.toNanos(64)),
                        TimeUnit.NANOSECONDS);
            }
        }
    }

    public Span getDelay() {
        return delay;
    }

    public long getNetMutations() {
        return mutationsProcessed - deletionsProcessed;
    }

    public long getMutationsProcessed() {
        return mutationsProcessed;
    }

    public long getDeletionsProcessed() {
        return deletionsProcessed;
    }

    @GuardedBy("operations on a vbucket do not interleave")
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void processDataEvent(ByteBuf event) {
        switch (event.getByte(1)) {
            case MessageUtil.DCP_DELETION_OPCODE:
            case MessageUtil.DCP_EXPIRATION_OPCODE:
                deletionsProcessed++;
                setSeqno(DcpDataMessage.bySeqno(event));
                break;
            case MessageUtil.DCP_MUTATION_OPCODE:
                mutationsProcessed++;
                setSeqno(DcpDataMessage.bySeqno(event));
                break;
            default:
                LOGGER.error("unrecognized data event {}", MessageUtil.humanize(event));
                throw new IllegalArgumentException("unrecognized data event: " + MessageUtil.humanize(event));
        }
    }
}

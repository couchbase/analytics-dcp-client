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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.message.DcpDataMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.MathUtil;
import com.couchbase.client.dcp.util.MemcachedStatus;

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

    private final StreamState streamState;
    private final short vbid;

    /**
     * Current Sequence Number
     */
    private volatile long seqno = INVALID_SEQNO;

    private volatile long streamEndSeq = 0;

    private volatile long snapshotStartSeqno = 0;

    private volatile long snapshotEndSeqno = 0;

    private volatile long purgeSeqno;

    private volatile byte state;

    private volatile long osoMaxSeqno = 0;

    private volatile long seqnoAdvances = 0;

    private volatile long mutationsProcessed = 0;

    private volatile long deletionsProcessed = 0;

    private volatile long extraneousSeqs = 0;

    private StreamRequest streamRequest;

    private long manifestUid;

    private Span delay = ELAPSED;

    /**
     * Initialize a new partition state.
     */
    public StreamPartitionState(StreamState streamState, short vbid) {
        this.streamState = streamState;
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
        streamState.getSessionState().ensureMaxCurrentVBucketSeqnoInMaster(vbid, snapshotEndSeqno);
        // TODO: this needs to be considered once we have >1 cid per stream
        for (int cid : streamState.cids()) {
            streamState.getSessionState().getCollectionState(cid).ensureMaxSeqno(vbid, snapshotEndSeqno);
        }
    }

    public void setPurgeSeqno(long purgeSeqno) {
        this.purgeSeqno = purgeSeqno;
    }

    /**
     * Returns the current sequence number.
     */
    public long getSeqno() {
        return seqno;
    }

    public long getPurgeSeqno() {
        return purgeSeqno;
    }

    /**
     * Allows to set the current sequence number.
     */
    @GuardedBy("operations on a vbucket do not interleave")
    @SuppressWarnings({ "NonAtomicOperationOnVolatileField", "java:S3078" })
    public void setSeqno(long seqno) {
        // disregard seqnos > streamEndSeq
        seqno = minSeqNo(streamEndSeq, seqno);
        if (state == CONNECTED_OSO) {
            osoMaxSeqno = maxSeqNo(seqno, osoMaxSeqno);
        } else {
            if (Long.compareUnsigned(seqno, this.seqno) < 0 && this.seqno != INVALID_SEQNO) {
                LOGGER.log(seqno == this.seqno ? Level.DEBUG : Level.WARN,
                        "new seqno received ({}) < the previous seqno({}) for vbid: {}", Long.toUnsignedString(seqno),
                        Long.toUnsignedString(this.seqno), vbid);
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("setting seqno to {} for vbid {} on setSeqno", seqno, vbid);
            }
            seqnoAdvances += this.seqno != INVALID_SEQNO ? (seqno - this.seqno) : seqno;
            this.seqno = seqno;
        }
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

    @GuardedBy("operations on a vbucket do not interleave")
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void setStreamRequest(StreamRequest streamRequest, boolean shouldLog) {
        this.streamRequest = streamRequest;
        seqno = streamRequest.getStartSeqno();
        streamEndSeq = streamRequest.getEndSeqno();
        snapshotStartSeqno = streamRequest.getSnapshotStartSeqno();
        snapshotEndSeqno = streamRequest.getSnapshotEndSeqno();
        purgeSeqno = streamRequest.getPurgeSeqno();
        manifestUid = streamRequest.getManifestUid();
        if (shouldLog && LOGGER.isDebugEnabled()) {
            LOGGER.debug("setStreamRequest: {}", streamRequest);
        }
        extraneousSeqs = 0;
    }

    public void prepareNextStreamRequest(SessionState sessionState, StreamState streamState) {
        if (streamRequest == null) {
            if (snapshotStartSeqno > seqno) {
                snapshotStartSeqno = seqno;
            }
            if (SessionState.NO_END_SEQNO != streamEndSeq && Long.compareUnsigned(streamEndSeq, seqno) < 0) {
                streamEndSeq = snapshotEndSeqno;
            }
            setStreamRequest(
                    new StreamRequest(vbid, seqno, streamEndSeq, sessionState.get(vbid).uuid(), snapshotStartSeqno,
                            snapshotEndSeqno, purgeSeqno, manifestUid, streamState.streamId(), streamState.cids()),
                    true);
        }
    }

    public short vbid() {
        return vbid;
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
            return "{\"" + this.getClass().getSimpleName() + "\":\"" + e + "\"}";
        }
    }

    public Map<String, Object> toMap() {
        Map<String, Object> tree = new HashMap<>();
        tree.put("vbid", vbid);
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

    public void setManifestUid(long manifestUid) {
        this.manifestUid = manifestUid;
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

    public StreamState getStreamState() {
        return streamState;
    }

    @GuardedBy("operations on a vbucket do not interleave")
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void processDataEvent(ByteBuf event) {
        long eventSeqno = DcpDataMessage.bySeqno(event);
        if (Long.compareUnsigned(eventSeqno, streamEndSeq) > 0) {
            // don't count mutations that occur after requested end seqno, as these will mess up our ingestion status
            LOGGER.trace("not counting mutation at seqno {} as it is extraneous since our stream end is {}", eventSeqno,
                    streamEndSeq);
            return;
        }
        switch (event.getByte(1)) {
            case MessageUtil.DCP_DELETION_OPCODE:
            case MessageUtil.DCP_EXPIRATION_OPCODE:
                deletionsProcessed++;
                break;
            case MessageUtil.DCP_MUTATION_OPCODE:
                mutationsProcessed++;
                break;
            default:
                LOGGER.error("unrecognized data event {}", MessageUtil.humanize(event));
                throw new IllegalArgumentException("unrecognized data event: " + MessageUtil.humanize(event));
        }
    }

    public void incPastEndSequence() {
        extraneousSeqs++;
    }

    /**
     * Returns the max seqno, where any valid seqno is considered higher than INVALID_SEQNO
     */
    public static long maxSeqNo(long a, long b) {
        return MathUtil.maxUnsigned(a + 1, b + 1) - 1;
    }

    public static long minSeqNo(long a, long b) {
        return MathUtil.minUnsigned(a, b);
    }
}

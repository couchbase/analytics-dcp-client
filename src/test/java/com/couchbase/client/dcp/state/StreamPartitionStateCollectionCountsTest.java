/*
 * Copyright 2026-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.dcp.message.MessageUtil;

/**
 * MB-74233: a stream carrying several collections counts the mutations and deletions of each apart, so that a
 * dataset's ingestion progress is its own collection's and not the whole stream's; a stream carrying a single
 * collection keeps only its totals, which are that collection's.
 */
public class StreamPartitionStateCollectionCountsTest {

    /** one LEB128 byte */
    private static final int CID_A = 0x8;
    /** two LEB128 bytes, so a cid decoded from only the first would not match */
    private static final int CID_B = 0x1a0;
    /** carried by neither stream */
    private static final int CID_OTHER = 0x9;
    private static final short VBID = 0;
    private static final String UUID = "0123456789abcdef0123456789abcdef";
    private static final String CONFIG = "{" //
            + "\"rev\":1,\"revEpoch\":1,\"name\":\"default\",\"uuid\":\"" + UUID + "\"," //
            + "\"uri\":\"/pools/default/buckets/default?bucket_uuid=" + UUID + "\"," //
            + "\"streamingUri\":\"/pools/default/bucketsStreaming/default?bucket_uuid=" + UUID + "\"," //
            + "\"nodeLocator\":\"vbucket\",\"bucketType\":\"membase\"," //
            + "\"nodes\":[{\"hostname\":\"127.0.0.1:8091\",\"ports\":{\"direct\":11210}}]," //
            + "\"nodesExt\":[{\"services\":{\"mgmt\":8091,\"kv\":11210},\"hostname\":\"127.0.0.1\",\"thisNode\":true}]," //
            + "\"vBucketServerMap\":{\"hashAlgorithm\":\"CRC\",\"numReplicas\":0,\"serverList\":[\"127.0.0.1:11210\"]," //
            + "\"vBucketMap\":[[0],[0],[0],[0]]}," //
            + "\"bucketCapabilities\":[\"dcp\",\"collections\"],\"collectionsManifestUid\":\"0\"}";

    private SessionState sessionState;
    private long seqno;

    @Before
    public void setUp() {
        CouchbaseBucketConfig config = (CouchbaseBucketConfig) BucketConfigParser.parse(CONFIG,
                StandardMemcachedHashingStrategy.INSTANCE, "127.0.0.1");
        sessionState = new SessionState(config);
        seqno = 0;
    }

    @Test
    public void eachCollectionOfASharedStreamIsCountedApart() {
        StreamPartitionState ps = partition(sessionState.newStream(1, new int[] { CID_A, CID_B }, VBID));
        process(ps, MessageUtil.DCP_MUTATION_OPCODE, CID_A, 3);
        process(ps, MessageUtil.DCP_MUTATION_OPCODE, CID_B, 5);
        process(ps, MessageUtil.DCP_DELETION_OPCODE, CID_B, 1);
        process(ps, MessageUtil.DCP_EXPIRATION_OPCODE, CID_A, 2);
        assertEquals(3, ps.getMutationsProcessed(CID_A));
        assertEquals(2, ps.getDeletionsProcessed(CID_A));
        assertEquals(5, ps.getMutationsProcessed(CID_B));
        assertEquals(1, ps.getDeletionsProcessed(CID_B));
        assertEquals(0, ps.getMutationsProcessed(CID_OTHER));
        // the totals remain every collection's together
        assertEquals(8, ps.getMutationsProcessed());
        assertEquals(3, ps.getDeletionsProcessed());
    }

    @Test
    public void eventsPastTheStreamEndAreNotCountedForAnyCollection() {
        StreamPartitionState ps = partition(sessionState.newStream(1, new int[] { CID_A, CID_B }, VBID));
        process(ps, MessageUtil.DCP_MUTATION_OPCODE, CID_A, 2);
        ps.setStreamEndSeq(seqno);
        process(ps, MessageUtil.DCP_MUTATION_OPCODE, CID_A, 4);
        assertEquals(2, ps.getMutationsProcessed(CID_A));
        assertEquals(2, ps.getMutationsProcessed());
    }

    @Test
    public void aSingleCollectionStreamReportsItsTotals() {
        StreamState stream = sessionState.newStream(1, new int[] { CID_A }, VBID);
        assertFalse(stream.countsPerCollection());
        StreamPartitionState ps = partition(stream);
        process(ps, MessageUtil.DCP_MUTATION_OPCODE, CID_A, 4);
        process(ps, MessageUtil.DCP_DELETION_OPCODE, CID_A, 1);
        assertEquals(4, ps.getMutationsProcessed(CID_A));
        assertEquals(1, ps.getDeletionsProcessed(CID_A));
    }

    @Test
    public void onlyAStreamOfSeveralCollectionsCountsPerCollection() {
        assertTrue(sessionState.newStream(1, new int[] { CID_A, CID_B }, VBID).countsPerCollection());
        assertFalse(sessionState.newStream(2, new int[] { CID_B }, VBID).countsPerCollection());
        assertEquals(-1, sessionState.streamState(2).cidIndex(CID_B));
    }

    private static StreamPartitionState partition(StreamState stream) {
        StreamPartitionState ps = stream.get(VBID);
        ps.setStreamEndSeq(-1L);
        return ps;
    }

    private void process(StreamPartitionState ps, byte opcode, int cid, int count) {
        for (int i = 0; i < count; i++) {
            ByteBuf event = dataEvent(opcode, cid, ++seqno);
            try {
                ps.processDataEvent(event);
            } finally {
                event.release();
            }
        }
    }

    private static ByteBuf dataEvent(byte opcode, int cid, long bySeqno) {
        ByteBuf event = Unpooled.buffer();
        MessageUtil.initRequest(opcode, event);
        MessageUtil.setVbucket(VBID, event);
        ByteBuf extras = Unpooled.buffer().writeLong(bySeqno).writeLong(1L);
        MessageUtil.setExtras(extras, event);
        extras.release();
        ByteBuf key = Unpooled.buffer();
        writeLEB128(cid, key);
        key.writeBytes("key".getBytes(CharsetUtil.UTF_8));
        MessageUtil.setKey(key, event);
        key.release();
        return event;
    }

    private static void writeLEB128(int value, ByteBuf out) {
        int remaining = value;
        do {
            int group = remaining & 0x7f;
            remaining >>>= 7;
            out.writeByte(remaining == 0 ? group : group | 0x80);
        } while (remaining != 0);
    }
}

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
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;

/**
 * MB-74653: a vbucket whose consumer already holds documents the seqno does not account for must be streamed from 0
 * with its deletes, as a deletion skipped in the initial backfill would leave one of those documents behind.
 */
public class StreamPartitionStateDeletesRequiredTest {

    private static final short VBID = 0;
    private static final int CID = 0x8;
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

    private StreamState stream;
    private StreamPartitionState ps;

    @Before
    public void setUp() {
        CouchbaseBucketConfig config = (CouchbaseBucketConfig) BucketConfigParser.parse(CONFIG,
                StandardMemcachedHashingStrategy.INSTANCE, "127.0.0.1");
        SessionState sessionState = new SessionState(config);
        stream = sessionState.newStream(1, new int[] { CID }, VBID);
        ps = stream.get(VBID);
        open(0);
    }

    private void open(long startSeqno) {
        ps.setStreamRequest(new StreamRequest(VBID, startSeqno, SessionState.NO_END_SEQNO, 0, startSeqno, startSeqno, 0,
                0, stream.streamId(), CID), false);
        ps.setState(StreamPartitionState.CONNECTED);
    }

    @Test
    public void interruptedOsoSnapshotFromZeroRequiresDeletes() {
        ps.beginOutOfOrder();
        ps.setSeqno(42);
        assertTrue(ps.isDeletesRequired());
        // the reopen reuses the request from 0, and must still be sent every deletion
        assertEquals(0, ps.getStreamRequest().getStartSeqno());
        open(0);
        assertTrue(ps.isDeletesRequired());
    }

    @Test
    public void completedOsoSnapshotClearsIt() {
        ps.beginOutOfOrder();
        ps.setSeqno(42);
        ps.setSeqno(7);
        assertEquals(42, ps.endOutOfOrder());
        assertFalse(ps.isDeletesRequired());
        assertEquals(42, ps.getSeqno());
    }

    @Test
    public void emptyOsoSnapshotDoesNotRequireDeletes() {
        ps.beginOutOfOrder();
        ps.endOutOfOrder();
        assertFalse(ps.isDeletesRequired());
    }

    @Test
    public void inOrderBackfillDoesNotRequireDeletes() {
        ps.setSeqno(3);
        assertFalse(ps.isDeletesRequired());
    }

    @Test
    public void osoSnapshotFromNonZeroDoesNotRequireDeletes() {
        open(10);
        ps.beginOutOfOrder();
        ps.setSeqno(42);
        assertFalse(ps.isDeletesRequired());
    }

    @Test
    public void requiredByTheConsumerOnlyAtZero() {
        ps.requireDeletes();
        assertTrue(ps.isDeletesRequired());
        ps.setSeqno(5);
        assertFalse(ps.isDeletesRequired());
        ps.requireDeletes();
        assertFalse(ps.isDeletesRequired());
    }

    @Test
    public void reopeningFromNonZeroClearsIt() {
        ps.requireDeletes();
        open(10);
        assertFalse(ps.isDeletesRequired());
    }
}

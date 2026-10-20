/*
 * Copyright 2026-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.conductor;

import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.state.StreamRequest;

/**
 * MB-74653: a dropped channel is replaced by a new {@link DcpChannel}, which has no open streams of its own when it
 * connects. Were the skip withdrawn only for the streams a channel already has open, the replacement would negotiate
 * it again for a vbucket which requires deletes, have the open of that vbucket's stream drop it, and be replaced in
 * turn, without end.
 */
public class DcpChannelSkipDeletesWithdrawalTest {

    private static final int SID = 1;
    private static final short VB = 1;
    private static final int[] CIDS = { 0x8 };
    private static final int NUM_VBUCKETS = 2;

    private static final String CONFIG = "{\"name\":\"b\",\"uuid\":\"0123456789abcdef\","
            + "\"uri\":\"/pools/default/buckets/b?bucket_uuid=0123456789abcdef\",\"nodeLocator\":\"vbucket\","
            + "\"bucketType\":\"membase\",\"nodes\":[{\"hostname\":\"127.0.0.1:8091\","
            + "\"ports\":{\"direct\":11210}}],\"nodesExt\":[{\"hostname\":\"127.0.0.1\","
            + "\"services\":{\"mgmt\":8091,\"kv\":11210}}],\"vBucketServerMap\":{\"hashAlgorithm\":\"CRC\","
            + "\"numReplicas\":0,\"serverList\":[\"127.0.0.1:11210\"],\"vBucketMap\":[[0],[0]]}}";

    @Test
    public void replacementChannelWithdrawsSkipForTaintedVbucket() {
        SessionState sessionState = sessionState();
        StreamPartitionState ps = sessionState.streamState(SID).get(VB);
        ps.requireDeletes();
        Assert.assertTrue(ps.isDeletesRequired());

        DcpChannel replacement = newChannel(sessionState);
        Assert.assertNull("a replacement has no open streams", replacement.openStreams()[VB]);
        replacement.withdrawSkipDeletesIfRequired();
        Assert.assertFalse(replacement.offersSkipDeletesInInitialBackfill());
    }

    @Test
    public void channelKeepsSkipWhenNothingRequiresDeletes() {
        DcpChannel channel = newChannel(sessionState());
        channel.withdrawSkipDeletesIfRequired();
        Assert.assertTrue(channel.offersSkipDeletesInInitialBackfill());
    }

    @Test
    public void sealedVbucketDoesNotWithdrawSkip() {
        SessionState sessionState = sessionState();
        StreamPartitionState ps = sessionState.streamState(SID).get(VB);
        ps.requireDeletes();
        ps.setState(StreamPartitionState.CONNECTED);
        Assert.assertTrue(ps.seal(0));
        DcpChannel channel = newChannel(sessionState);
        channel.withdrawSkipDeletesIfRequired();
        Assert.assertTrue(channel.offersSkipDeletesInInitialBackfill());
    }

    private static SessionState sessionState() {
        CouchbaseBucketConfig config = (CouchbaseBucketConfig) BucketConfigParser.parse(CONFIG,
                StandardMemcachedHashingStrategy.INSTANCE, "127.0.0.1");
        SessionState sessionState = new SessionState(config);
        sessionState.newStream(SID, CIDS, VB).get(VB).setStreamRequest(
                new StreamRequest(VB, 0, SessionState.NO_END_SEQNO, 0, 0, 0, 0, 0, SID, CIDS[0]), false);
        return sessionState;
    }

    private static DcpChannel newChannel(SessionState sessionState) {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 11210);
        ClientEnvironment env = ClientEnvironment.builder().setClusterAt(Collections.singletonList(address))
                .setSkipDeletesInInitialBackfill(true).build();
        return new DcpChannel(address, "127.0.0.1", env, sessionState, NUM_VBUCKETS, true);
    }
}

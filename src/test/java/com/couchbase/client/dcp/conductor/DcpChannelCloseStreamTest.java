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

import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.message.DcpCloseStreamRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamPartitionState;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

/**
 * {@link DcpChannel#closeStream} is either sent and recorded, or neither (MB-73569). A caller which sees it throw may
 * take it that nothing was sent; that holds only if nothing was recorded either, so the stream must not be marked
 * {@code DISCONNECTING} or dropped from the channel's open streams unless the close request went to the wire.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class DcpChannelCloseStreamTest {

    private static final int SID = 1;
    private static final short VB = 1;
    private static final int[] CIDS = { 0x8 };
    private static final int NUM_VBUCKETS = 2;

    // the smallest config the parser accepts which yields NUM_VBUCKETS partitions; only the partition count and the
    // bucket uuid are consumed here
    private static final String CONFIG = "{\"name\":\"b\",\"uuid\":\"0123456789abcdef\","
            + "\"uri\":\"/pools/default/buckets/b?bucket_uuid=0123456789abcdef\",\"nodeLocator\":\"vbucket\","
            + "\"bucketType\":\"membase\",\"nodes\":[{\"hostname\":\"127.0.0.1:8091\","
            + "\"ports\":{\"direct\":11210}}],\"nodesExt\":[{\"hostname\":\"127.0.0.1\","
            + "\"services\":{\"mgmt\":8091,\"kv\":11210}}],\"vBucketServerMap\":{\"hashAlgorithm\":\"CRC\","
            + "\"numReplicas\":0,\"serverList\":[\"127.0.0.1:11210\"],\"vBucketMap\":[[0],[0]]}}";

    /**
     * A channel whose write path fails synchronously, the way the Jira's residual case- an allocation or encoding
     * failure between recording the close and writing it- surfaces to the caller.
     */
    private static final class ThrowingChannel extends EmbeddedChannel {
        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            ReferenceCountUtil.release(msg);
            throw new IllegalStateException("simulated write failure");
        }
    }

    @Test
    public void testFailedWriteRecordsNothing() {
        SessionState sessionState = sessionState();
        DcpChannel dcpChannel = connectedChannel(sessionState, new ThrowingChannel());
        StreamPartitionState partition = sessionState.streamState(SID).get(VB);

        try {
            dcpChannel.closeStream(SID, VB);
            Assert.fail("expected the write failure to propagate");
        } catch (IllegalStateException e) {
            Assert.assertEquals("simulated write failure", e.getMessage());
        }
        // nothing was sent, so nothing may have been recorded: the stream is still open as far as we know, and KV is
        // still streaming it
        Assert.assertEquals("partition state", StreamPartitionState.CONNECTED, partition.getState());
        Assert.assertTrue("still an open stream", dcpChannel.openStreams()[VB].contains(SID));
    }

    @Test
    public void testSuccessfulWriteRecordsTheClose() {
        SessionState sessionState = sessionState();
        EmbeddedChannel channel = new EmbeddedChannel();
        DcpChannel dcpChannel = connectedChannel(sessionState, channel);
        StreamPartitionState partition = sessionState.streamState(SID).get(VB);

        dcpChannel.closeStream(SID, VB);

        ByteBuf written = channel.readOutbound();
        Assert.assertNotNull("a close request was written", written);
        try {
            Assert.assertTrue("a close stream request", DcpCloseStreamRequest.is(written));
            Assert.assertEquals("for the vbucket", VB, MessageUtil.getVbucket(written));
        } finally {
            written.release();
        }
        Assert.assertNull("exactly one request", channel.readOutbound());
        Assert.assertEquals("partition state", StreamPartitionState.DISCONNECTING, partition.getState());
        Assert.assertFalse("no longer an open stream", dcpChannel.openStreams()[VB].contains(SID));
    }

    @Test
    public void testNotConnectedRecordsNothing() {
        SessionState sessionState = sessionState();
        EmbeddedChannel channel = new EmbeddedChannel();
        DcpChannel dcpChannel = connectedChannel(sessionState, channel);
        dcpChannel.setState(State.DISCONNECTED);
        StreamPartitionState partition = sessionState.streamState(SID).get(VB);

        try {
            dcpChannel.closeStream(SID, VB);
            Assert.fail("expected NotConnectedException");
        } catch (NotConnectedException e) {
            // expected
        }
        Assert.assertNull("nothing written", channel.readOutbound());
        Assert.assertEquals("partition state", StreamPartitionState.CONNECTED, partition.getState());
        Assert.assertTrue("still an open stream", dcpChannel.openStreams()[VB].contains(SID));
    }

    @Test
    public void testUnknownVbucketWritesNothing() {
        SessionState sessionState = sessionState();
        EmbeddedChannel channel = new EmbeddedChannel();
        DcpChannel dcpChannel = connectedChannel(sessionState, channel);
        StreamPartitionState partition = sessionState.streamState(SID).get(VB);
        // the stream covers VB only, so on OTHER_VB it has neither a partition state nor an open-stream set: the pair
        // of dereferences which followed the write before, and which must now fail before it
        final short otherVb = (short) (NUM_VBUCKETS - 1 - VB);
        Assert.assertNull(sessionState.streamState(SID).get(otherVb));
        Assert.assertNull(dcpChannel.openStreams()[otherVb]);

        try {
            dcpChannel.closeStream(SID, otherVb);
            Assert.fail("expected an unknown vbucket to be rejected before anything is written");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("has no state on vbucket " + otherVb));
        }
        Assert.assertNull("nothing written", channel.readOutbound());
        Assert.assertEquals("partition state", StreamPartitionState.CONNECTED, partition.getState());
        Assert.assertTrue("still an open stream", dcpChannel.openStreams()[VB].contains(SID));
    }

    private static SessionState sessionState() {
        CouchbaseBucketConfig config = (CouchbaseBucketConfig) BucketConfigParser.parse(CONFIG,
                StandardMemcachedHashingStrategy.INSTANCE, "127.0.0.1");
        Assert.assertEquals(NUM_VBUCKETS, config.numberOfPartitions());
        SessionState sessionState = new SessionState(config);
        sessionState.newStream(SID, CIDS, VB).get(VB).setState(StreamPartitionState.CONNECTED);
        return sessionState;
    }

    /**
     * A {@link DcpChannel} in the state {@link DcpChannel#openStream} leaves it in once the open has been acknowledged:
     * connected, with {@link #SID} open on {@link #VB} and its partition {@code CONNECTED}.
     */
    private static DcpChannel connectedChannel(SessionState sessionState, EmbeddedChannel channel) {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 11210);
        ClientEnvironment env = ClientEnvironment.builder().setClusterAt(Collections.singletonList(address)).build();
        DcpChannel dcpChannel = new DcpChannel(address, "127.0.0.1", env, sessionState, NUM_VBUCKETS, true);
        dcpChannel.setChannel(channel);
        dcpChannel.openStreams()[VB] = new IntOpenHashSet(new int[] { SID });
        dcpChannel.setState(State.CONNECTED);
        return dcpChannel;
    }
}

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

import static org.apache.hyracks.util.annotations.AiProvenance.Agent.CLAUDE_FABLE_5;
import static org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.TEST_GENERATED;
import static org.apache.hyracks.util.annotations.AiProvenance.Tool.CLAUDE_CODE_UI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.util.Span;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Before;
import org.junit.Test;

import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpCloseStreamRequest;
import com.couchbase.client.dcp.message.DcpOpenStreamRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.util.MemcachedStatus;

/**
 * MB-73588: a disconnect which lands while streams are still being opened must still get every stream the producer
 * has closed. The producer handles a connection's requests in order, so a stream whose open stream response has not
 * yet reached us has nonetheless already been created (or rejected) on its side; dropping the connection with it open
 * ends it as an abnormal disconnect, which is what {@link DcpChannel#closeStreams()} exists to prevent.
 */
@AiProvenance(agent = CLAUDE_FABLE_5, tool = CLAUDE_CODE_UI, contributionKind = TEST_GENERATED, notes = "MB-73588 (Claude Fable 5.1)")
public class DcpChannelCloseStreamsTest {

    private static final int STREAM_ID = 1;
    private static final short NUM_VBUCKETS = 4;
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

    private EmbeddedChannel netty;
    private DcpChannel channel;
    private SessionState sessionState;
    private DcpChannelControlMessageHandler handler;

    @Before
    public void setUp() {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 11210);
        ClientEnvironment env = ClientEnvironment.builder().setClusterAt(new ArrayList<>(Arrays.asList(address)))
                .setBucket("default").setDcpControl(new DcpControl()).build();
        // the channel's handler passes every control message on to the client's, which owns the buffer from then on
        env.setControlEventHandler((ackHandle, buf) -> buf.release());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig) BucketConfigParser.parse(CONFIG,
                StandardMemcachedHashingStrategy.INSTANCE, "127.0.0.1");
        sessionState = new SessionState(config);
        sessionState.newStream(STREAM_ID, new int[] { 0 });
        channel = new DcpChannel(address, "127.0.0.1", env, sessionState, NUM_VBUCKETS, true);
        netty = new EmbeddedChannel();
        channel.setChannel(netty);
        channel.setState(State.CONNECTED);
        handler = new DcpChannelControlMessageHandler(channel);
        // request every stream; none of the open stream responses has arrived yet
        for (short vbid = 0; vbid < NUM_VBUCKETS; vbid++) {
            channel.openStream(vbid, 0, 0, Long.MAX_VALUE, 0, 0, 0, 0, STREAM_ID, new int[] { 0 });
        }
        assertEquals(vbids(0, 1, 2, 3), requests(DcpOpenStreamRequest::is));
        assertState(StreamPartitionState.CONNECTING, 0, 1, 2, 3);
    }

    /**
     * The scenario of MB-73588: two streams are established, two have their open stream response in flight when the
     * close is requested. The established streams are closed at once, and each in-flight stream is closed as soon as
     * its response reports it open, or settled without a close if the open failed.
     */
    @Test
    public void closesStreamsWhoseOpenCompletesDuringTheWait() {
        opened(0);
        opened(1);
        Collection<StreamPartitionState> closing = channel.closeStreams();
        // every stream is handed back to be awaited, not only the established ones...
        assertEquals(NUM_VBUCKETS, closing.size());
        // ...but only the established ones can be closed at once
        assertEquals(vbids(0, 1), requests(DcpCloseStreamRequest::is));
        assertState(StreamPartitionState.DISCONNECTING, 0, 1);
        assertState(StreamPartitionState.CONNECTING, 2, 3);
        Map<DcpChannel, Collection<StreamPartitionState>> awaiting = Collections.singletonMap(channel, closing);

        // nothing has arrived yet: still waiting, nothing further requested
        assertFalse(DcpChannel.awaitStreamsClosed(awaiting, elapsed()));
        assertEquals(vbids(), requests(DcpCloseStreamRequest::is));

        // the open of vbucket 2 is reported successful: the producer has that stream, so it is closed too
        opened(2);
        assertFalse(DcpChannel.awaitStreamsClosed(awaiting, elapsed()));
        assertEquals(vbids(2), requests(DcpCloseStreamRequest::is));
        assertState(StreamPartitionState.DISCONNECTING, 0, 1, 2);

        // the open of vbucket 3 is rejected: the producer has no such stream, so there is nothing to close
        openFailed(3);
        assertState(StreamPartitionState.DISCONNECTED, 3);

        // the producer reports the three closes done, and the wait is over with nothing left behind
        closed(0);
        closed(1);
        closed(2);
        assertTrue(DcpChannel.awaitStreamsClosed(awaiting, elapsed()));
        assertEquals(vbids(), requests(DcpCloseStreamRequest::is));
        assertState(StreamPartitionState.DISCONNECTED, 0, 1, 2, 3);
    }

    /**
     * An open which completes after the producer has dropped the connection has nobody left to send the close to;
     * the stream is settled rather than waited on, as is every stream still awaiting its open on that connection.
     */
    @Test
    public void settlesStreamsWhoseConnectionIsGone() {
        Collection<StreamPartitionState> closing = channel.closeStreams();
        assertEquals(NUM_VBUCKETS, closing.size());
        assertEquals(vbids(), requests(DcpCloseStreamRequest::is));
        Map<DcpChannel, Collection<StreamPartitionState>> awaiting = Collections.singletonMap(channel, closing);

        opened(0);
        netty.close();
        assertFalse(DcpChannel.awaitStreamsClosed(awaiting, elapsed()));
        assertEquals(vbids(), requests(DcpCloseStreamRequest::is));
        assertState(StreamPartitionState.DISCONNECTED, 0);
        assertState(StreamPartitionState.CONNECTING, 1, 2, 3);
    }

    @Test
    public void closesNothingOnceTheConnectionIsGone() {
        opened(0);
        netty.close();
        assertTrue(channel.closeStreams().isEmpty());
        assertEquals(vbids(), requests(DcpCloseStreamRequest::is));
    }

    private void opened(int vbid) {
        // a successful open stream response carries the failover log: one (uuid, seqno) entry
        ByteBuf failoverLog = Unpooled.buffer().writeLong(0x1234L).writeLong(0L);
        handler.onEvent(null,
                response(MessageUtil.DCP_STREAM_REQUEST_OPCODE, (short) vbid, MemcachedStatus.SUCCESS, failoverLog));
        assertState(StreamPartitionState.CONNECTED, vbid);
    }

    private void openFailed(int vbid) {
        handler.onEvent(null,
                response(MessageUtil.DCP_STREAM_REQUEST_OPCODE, (short) vbid, MemcachedStatus.NOT_MY_VBUCKET, null));
    }

    private void closed(int vbid) {
        handler.onEvent(null,
                response(MessageUtil.DCP_STREAM_CLOSE_OPCODE, (short) vbid, MemcachedStatus.SUCCESS, null));
    }

    private static ByteBuf response(byte opcode, short vbid, short status, ByteBuf content) {
        ByteBuf buf = Unpooled.buffer();
        MessageUtil.initResponse(opcode, buf);
        // the vbucket & stream id are round-tripped via the opaque, as the producer does
        MessageUtil.setOpaque(STREAM_ID << 16 | vbid, buf);
        buf.setShort(MessageUtil.VBUCKET_OFFSET, status);
        if (content != null) {
            MessageUtil.setContent(content, buf);
        }
        return buf;
    }

    /**
     * Drains the requests written to the connection, returning the vbuckets of those of the supplied kind (and
     * asserting there were no others).
     */
    private List<Short> requests(java.util.function.Predicate<ByteBuf> kind) {
        List<Short> vbids = new ArrayList<>();
        ByteBuf request;
        while ((request = netty.readOutbound()) != null) {
            assertTrue("unexpected request: " + MessageUtil.humanize(request), kind.test(request));
            vbids.add(MessageUtil.getVbucket(request));
            request.release();
        }
        Collections.sort(vbids);
        return vbids;
    }

    private static List<Short> vbids(int... vbids) {
        List<Short> list = new ArrayList<>();
        for (int vbid : vbids) {
            list.add((short) vbid);
        }
        return list;
    }

    private void assertState(byte expected, int... vbids) {
        for (int vbid : vbids) {
            assertEquals("vbucket " + vbid, expected, sessionState.streamState(STREAM_ID).get((short) vbid).getState());
        }
    }

    /**
     * A span which has already elapsed, so that {@link DcpChannel#awaitStreamsClosed(Map, Span)} makes exactly one
     * pass: acting on whatever responses have arrived, and then reporting whether anything is still outstanding.
     */
    private static Span elapsed() {
        return Span.start(0, TimeUnit.MILLISECONDS);
    }
}

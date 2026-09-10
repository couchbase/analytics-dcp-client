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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.dcp.config.DcpControl;

/**
 * The purge seqno we send on a stream request is how a rollback is avoided when a stream reopens: the producer compares
 * it against its own purge seqno and only forces a rollback if we are behind one we have not processed. It is therefore
 * worth being precise about when it is sent, and about what is sent when it is not.
 * <p>
 * Both halves are easy to get wrong invisibly. The feature is gated on a negotiated capability, so a deployment can have
 * it enabled and still never send anything, and a request which sends 0 does not mean "no opinion"- it positively
 * asserts that no purge has been processed, which is the input most likely to earn a rollback.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class DcpChannelPurgeSeqnoTest {

    private static final int[] CIDS = { 0x0, 0x8 };
    private static final long PURGE_SEQNO = 4000L;

    // ---------------------------------------------------------------- the negotiated gate

    @Test
    public void testSentOnlyWhenEnabledAndV22Negotiated() {
        Assert.assertTrue(DcpChannel.shouldIncludePurgeSeqnos(true, negotiated(DcpControl.MAX_MARKER_VERSION_2_2)));
    }

    @Test
    public void testNotSentWhenDisabledInTheEnvironment() {
        // the environment switch alone decides nothing; a disabled deployment must not send them however capable the
        // producer is
        Assert.assertFalse(DcpChannel.shouldIncludePurgeSeqnos(false, negotiated(DcpControl.MAX_MARKER_VERSION_2_2)));
    }

    @Test
    public void testNotSentWhenTheProducerDidNotNegotiateAMarkerVersion() {
        // an older producer simply does not accept the control, so the setting is absent rather than false. This is the
        // case that silently disabled the feature outright when the control was never requested
        Assert.assertFalse(DcpChannel.shouldIncludePurgeSeqnos(true, negotiated(null)));
    }

    @Test
    public void testNotSentForAnyMarkerVersionOtherThan22() {
        // only 2.2 reports a purge seqno on the marker, so only 2.2 gives us one to echo back. 2.0 is a marker version
        // which exists and is not sufficient, which is why this is an equality check and not a "supports v2" check
        for (String version : new String[] { "1", "2", "2.0", "2.1", "2.20", "", " 2.2", "2.2 " }) {
            Assert.assertFalse("unexpectedly enabled for marker version '" + version + "'",
                    DcpChannel.shouldIncludePurgeSeqnos(true, negotiated(version)));
        }
    }

    // ---------------------------------------------------------------- what reaches the request

    @Test
    public void testPurgeSeqnoIsSentWhenNonZeroAndPermitted() {
        ObjectNode json = value(PURGE_SEQNO, true);
        Assert.assertEquals("4000", json.path("purge_seqno").asText());
    }

    @Test
    public void testPurgeSeqnoIsOmittedWhenTheGateIsClosed() {
        // a producer which cannot honour it must not be sent it at all
        Assert.assertFalse(value(PURGE_SEQNO, false).has("purge_seqno"));
    }

    @Test
    public void testAZeroPurgeSeqnoIsOmittedRatherThanDeclared() {
        // the distinction this pins: omitting says "no purge to report", whereas sending 0 asserts that we have
        // processed no purge whatsoever against a vbucket which may well have one- and asks for a rollback by doing so
        Assert.assertFalse(value(0L, true).has("purge_seqno"));
    }

    @Test
    public void testPurgeSeqnoIsRenderedUnsigned() {
        // seqnos are unsigned 64-bit; rendered signed, a high one becomes negative and the producer reads nonsense
        long highSeqno = -2L; // 0xFFFF_FFFF_FFFF_FFFE
        Assert.assertEquals("18446744073709551614", value(highSeqno, true).path("purge_seqno").asText());
    }

    @Test
    public void testTheRestOfTheRequestIsUnaffected() {
        // the purge seqno is an addition to an existing value, so the fields which were always there must be untouched
        ObjectNode json = DcpChannel.streamRequestValue(new ObjectMapper(), CIDS, 0x1L, 3, PURGE_SEQNO, true);
        Assert.assertEquals("[\"0\",\"8\"]", json.path("collections").toString());
        Assert.assertEquals("1", json.path("uid").asText());
        Assert.assertEquals(3, json.path("sid").asInt());
    }

    @Test
    public void testStreamIdAndManifestUidAreOmittedWhenUnset() {
        ObjectNode json = DcpChannel.streamRequestValue(new ObjectMapper(), CIDS, 0L, 0, PURGE_SEQNO, true);
        Assert.assertFalse("uid must be absent when the manifest uid is 0", json.has("uid"));
        Assert.assertFalse("sid must be absent when there is no stream id", json.has("sid"));
        Assert.assertTrue("the purge seqno is independent of those", json.has("purge_seqno"));
    }

    private static ObjectNode value(long purgeSeqno, boolean includePurgeSeqnos) {
        return DcpChannel.streamRequestValue(new ObjectMapper(), CIDS, 0x1L, 3, purgeSeqno, includePurgeSeqnos);
    }

    /**
     * @param markerVersion the negotiated max marker version, or {@code null} for a producer which did not accept the
     *                      control at all
     */
    private static Map<String, String> negotiated(String markerVersion) {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put(DcpControl.Names.ENABLE_NOOP.value(), "true");
        if (markerVersion != null) {
            settings.put(DcpControl.Names.MAX_MARKER_VERSION.value(), markerVersion);
        }
        return settings;
    }
}

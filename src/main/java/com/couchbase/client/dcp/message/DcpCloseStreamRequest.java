/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_CLOSE_OPCODE;

import org.apache.hyracks.util.annotations.AiProvenance;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

public enum DcpCloseStreamRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_STREAM_CLOSE_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(DCP_STREAM_CLOSE_OPCODE, buffer);
    }

    /**
     * Initialize a close stream request for a connection which has negotiated
     * {@link com.couchbase.client.dcp.config.DcpControl.Names#ENABLE_STREAM_ID}; such a connection <i>requires</i>
     * the stream id to be supplied, and the server rejects the request with
     * {@link com.couchbase.client.dcp.util.MemcachedStatus#STREAMID_INVALID} otherwise.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI)
    public static void init(final ByteBuf buffer, final int streamId) {
        MessageUtil.initFlexRequestWithStreamId(DCP_STREAM_CLOSE_OPCODE, streamId, buffer);
    }

    public static void vbucket(final ByteBuf buffer, final short vbid) {
        MessageUtil.setVbucket(vbid, buffer);
    }

    /**
     * Round-trips the vbucket & stream id via the opaque, as the server does not otherwise identify the stream in its
     * response. Mirrors {@link DcpOpenStreamRequest#vbucketStreamId(ByteBuf, short, int)}.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Replaces opaque(ByteBuf, int), which round-tripped the vbucket alone")
    public static void vbucketStreamId(final ByteBuf buffer, final short vbid, final int streamId) {
        MessageUtil.setOpaque(streamId << 16 | vbid, buffer);
    }

}

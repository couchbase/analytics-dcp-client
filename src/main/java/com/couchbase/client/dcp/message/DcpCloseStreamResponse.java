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

public enum DcpCloseStreamResponse {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_STREAM_CLOSE_OPCODE;
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, notes = "The vbucket & stream id are round-tripped via the opaque, as with open stream")
    public static short vbucket(final ByteBuf buffer) {
        return (short) (MessageUtil.getOpaque(buffer) & 0xffff);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, notes = "The vbucket & stream id are round-tripped via the opaque, as with open stream")
    public static int streamId(final ByteBuf buffer) {
        return MessageUtil.getOpaque(buffer) >> 16;
    }

}

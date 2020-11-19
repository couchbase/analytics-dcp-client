/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_REQUEST_OPCODE;

import com.couchbase.client.dcp.util.MemcachedStatus;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpOpenStreamResponse {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_STREAM_REQUEST_OPCODE;
    }

    public static short vbucket(ByteBuf buffer) {
        return (short) (MessageUtil.getOpaque(buffer) & 0xffff);
    }

    public static int streamId(ByteBuf buffer) {
        return MessageUtil.getOpaque(buffer) >> 16;
    }

    public static long rollbackSeqno(ByteBuf buffer) {
        if (MessageUtil.getStatus(buffer) == MemcachedStatus.ROLLBACK) {
            return MessageUtil.getContent(buffer).getLong(0);
        } else {
            throw new IllegalStateException("Rollback sequence number accessible only for ROLLBACK (0x23) status code");
        }
    }
}

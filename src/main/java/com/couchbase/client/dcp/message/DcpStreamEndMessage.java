/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_END_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpStreamEndMessage {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_STREAM_END_OPCODE;
    }

    public static short vbucket(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static StreamEndReason reason(final ByteBuf buffer) {
        int flags = MessageUtil.getExtras(buffer).getInt(0);
        return StreamEndReason.of(flags);
    }
}

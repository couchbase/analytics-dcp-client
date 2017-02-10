/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_CLOSE_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpCloseStreamResponse {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_STREAM_CLOSE_OPCODE;
    }

}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_ADD_STREAM_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

public enum DcpAddStreamResponse {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_ADD_STREAM_OPCODE;
    }

    public static void init(final ByteBuf buffer, int opaque) {
        MessageUtil.initResponse(DCP_ADD_STREAM_OPCODE, buffer);
        opaque(buffer, opaque);
    }

    /**
     * The opaque field contains the opaque value used by messages passing for that VBucket.
     */
    public static int opaque(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getInt(0);
    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        ByteBuf extras = Unpooled.buffer(4);
        MessageUtil.setExtras(extras.writeInt(opaque), buffer);
        extras.release();
    }
}

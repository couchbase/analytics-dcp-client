/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_BUFFER_ACK_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

public enum DcpBufferAckRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_BUFFER_ACK_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(DCP_BUFFER_ACK_OPCODE, buffer);
    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        MessageUtil.setOpaque(opaque, buffer);
    }

    public static void ackBytes(final ByteBuf buffer, int bytes) {
        ByteBuf extras = Unpooled.buffer(4);
        MessageUtil.setExtras(extras.writeInt(bytes), buffer);
        extras.release();
    }

}

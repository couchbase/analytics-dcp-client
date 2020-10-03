/*
 * Copyright (c) 2020 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class DcpSeqnoAdvancedMessage {
    private DcpSeqnoAdvancedMessage() {
        throw new AssertionError("not instantiable");
    }

    public static boolean is(ByteBuf event) {
        return event.getByte(0) == MessageUtil.MAGIC_REQ && event.getByte(1) == MessageUtil.DCP_SEQNO_ADVANCED_OPCODE;
    }

    public static short vbucket(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static long getSeqno(ByteBuf event) {
        return MessageUtil.getExtras(event).readLong();
    }
}

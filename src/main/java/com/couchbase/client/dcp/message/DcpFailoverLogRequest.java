/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_FAILOVER_LOG_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpFailoverLogRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_FAILOVER_LOG_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(DCP_FAILOVER_LOG_OPCODE, buffer);
    }

    public static void vbucket(final ByteBuf buffer, final short vbid) {
        MessageUtil.setVbucket(vbid, buffer);
    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        MessageUtil.setOpaque(opaque, buffer);
    }

}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.OPEN_CONNECTION_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpOpenConnectionResponse {
    ;

    /**
     * If the given buffer is a {@link DcpOpenConnectionResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == OPEN_CONNECTION_OPCODE;
    }

}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_LIST_MECHS_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum SaslListMechsRequest {
    ;

    /**
     * If the given buffer is a {@link SaslListMechsRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == SASL_LIST_MECHS_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     */
    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(SASL_LIST_MECHS_OPCODE, buffer);
    }

}

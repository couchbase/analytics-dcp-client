/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_AUTH_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum SaslAuthResponse {
    ;

    /**
     * If the given buffer is a {@link SaslAuthResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == SASL_AUTH_OPCODE;
    }

    /**
     * Returns the server challenge.
     */
    public static ByteBuf challenge(final ByteBuf buffer) {
        return MessageUtil.getContent(buffer);
    }

}

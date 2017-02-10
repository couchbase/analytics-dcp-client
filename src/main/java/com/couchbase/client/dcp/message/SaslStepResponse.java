/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_STEP_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum SaslStepResponse {
    ;

    /**
     * If the given buffer is a {@link SaslStepResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == SASL_STEP_OPCODE;
    }

}

/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_STEP_OPCODE;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

public enum SaslStepRequest {
    ;

    /**
     * If the given buffer is a {@link SaslStepRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == SASL_STEP_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     */
    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(SASL_STEP_OPCODE, buffer);
    }

    /**
     * Sets the selected mechanism.
     */
    public static void mechanism(ByteBuf mechanism, ByteBuf buffer) {
        MessageUtil.setKey(mechanism, buffer);
    }

    /**
     * Sets the challenge response payload.
     */
    public static void challengeResponse(ByteBuf challengeResponse, ByteBuf buffer) {
        MessageUtil.setContent(challengeResponse, buffer);
    }

}

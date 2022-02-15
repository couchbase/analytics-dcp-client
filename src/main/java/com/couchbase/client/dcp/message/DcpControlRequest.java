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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_CONTROL_OPCODE;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

public enum DcpControlRequest {
    ;

    /**
     * If the given buffer is a {@link DcpControlRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_CONTROL_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     *
     * Note that this will implicitly set the flags to "consumer".
     */
    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(DCP_CONTROL_OPCODE, buffer);
    }

    public static void key(final ByteBuf key, final ByteBuf buffer) {
        MessageUtil.setKey(key, buffer);
    }

    public static void value(final ByteBuf value, final ByteBuf buffer) {
        MessageUtil.setContent(value, buffer);
    }
}

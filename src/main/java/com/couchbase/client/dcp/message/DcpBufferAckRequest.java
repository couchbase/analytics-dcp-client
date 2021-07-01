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

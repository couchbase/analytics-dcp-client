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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_ADD_STREAM_OPCODE;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

public enum DcpAddStreamResponse {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_ADD_STREAM_OPCODE;
    }

    public static void init(final ByteBuf buffer, int opaque) {
        MessageUtil.initResponse(DCP_ADD_STREAM_OPCODE, buffer);
        opaque(buffer, opaque);
    }

    /**
     * The opaque field contains the opaque value used by messages passing for that VBucket.
     */
    public static int opaque(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getInt(0);
    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        ByteBuf extras = Unpooled.buffer(4);
        MessageUtil.setExtras(extras.writeInt(opaque), buffer);
        extras.release();
    }
}

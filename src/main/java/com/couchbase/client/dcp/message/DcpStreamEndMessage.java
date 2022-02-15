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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_END_OPCODE;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

public enum DcpStreamEndMessage {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_STREAM_END_OPCODE;
    }

    public static short vbucket(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static StreamEndReason reason(final ByteBuf buffer) {
        int flags = MessageUtil.getExtras(buffer).getInt(0);
        return StreamEndReason.of(flags);
    }
}

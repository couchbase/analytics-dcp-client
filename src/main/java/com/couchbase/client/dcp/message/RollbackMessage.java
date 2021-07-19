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

import static com.couchbase.client.dcp.message.MessageUtil.INTERNAL_ROLLBACK_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum RollbackMessage {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_INT && buffer.getByte(1) == INTERNAL_ROLLBACK_OPCODE;
    }

    public static void init(ByteBuf buffer, short vbid, long seqno) {
        buffer.writeByte(MessageUtil.MAGIC_INT);
        buffer.writeByte(MessageUtil.INTERNAL_ROLLBACK_OPCODE);
        buffer.writeShort(vbid);
        buffer.writeLong(seqno);
    }

    public static short vbucket(ByteBuf buffer) {
        return buffer.getShort(2);
    }

    public static long seqno(ByteBuf buffer) {
        return buffer.getLong(4);
    }

    public static String toString(ByteBuf buffer) {
        return "Rollback [vbid: " + vbucket(buffer) + ", seqno: " + seqno(buffer) + "]";
    }

}

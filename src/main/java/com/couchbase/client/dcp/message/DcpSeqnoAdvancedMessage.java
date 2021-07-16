/*
 * Copyright 2020-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class DcpSeqnoAdvancedMessage {
    private DcpSeqnoAdvancedMessage() {
        throw new AssertionError("not instantiable");
    }

    public static boolean is(ByteBuf event) {
        return event.getByte(0) == MessageUtil.MAGIC_REQ && event.getByte(1) == MessageUtil.DCP_SEQNO_ADVANCED_OPCODE;
    }

    public static short vbucket(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static long getSeqno(ByteBuf event) {
        return MessageUtil.getExtras(event).readLong();
    }
}

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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_DELETION_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class DcpDeletionMessage extends DcpDataMessage {

    private DcpDeletionMessage() {
        throw new AssertionError("do not instantiate");
    }

    public static boolean is(final ByteBuf buffer) {
        final byte magic = buffer.getByte(0);
        return buffer.getByte(1) == DCP_DELETION_OPCODE
                && (magic == MessageUtil.MAGIC_REQ || magic == MessageUtil.MAGIC_REQ_FLEX);
    }

    public static String toString(final ByteBuf buffer, boolean isCollectionEnabled) {
        return "DeletionMessage [key: \"" + keyString(buffer, isCollectionEnabled) + "\", vbid: " + partition(buffer)
                + ", cas: " + cas(buffer) + ", bySeqno: " + bySeqno(buffer) + ", revSeqno: " + revisionSeqno(buffer)
                + "]";
    }
}

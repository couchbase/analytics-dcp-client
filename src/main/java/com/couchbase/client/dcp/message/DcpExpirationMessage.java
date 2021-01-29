/*
 * Copyright (c) 2016-2021 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_EXPIRATION_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class DcpExpirationMessage extends DcpDataMessage {

    private DcpExpirationMessage() {
        throw new AssertionError("do not instantiate");
    }

    public static boolean is(final ByteBuf buffer) {
        final byte magic = buffer.getByte(0);
        return buffer.getByte(1) == DCP_EXPIRATION_OPCODE
                && (magic == MessageUtil.MAGIC_REQ || magic == MessageUtil.MAGIC_REQ_FLEX);
    }

    public static String toString(final ByteBuf buffer, boolean isCollectionEnabled) {
        return "ExpirationMessage [key: \"" + keyString(buffer, isCollectionEnabled) + "\", vbid: " + partition(buffer)
                + ", cas: " + cas(buffer) + ", bySeqno: " + bySeqno(buffer) + ", revSeqno: " + revisionSeqno(buffer)
                + "]";
    }
}

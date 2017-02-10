/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_DELETION_OPCODE;

import java.nio.charset.Charset;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

public enum DcpDeletionMessage {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_DELETION_OPCODE;
    }

    public static ByteBuf key(final ByteBuf buffer) {
        return MessageUtil.getKey(buffer);
    }

    public static String keyString(final ByteBuf buffer, Charset charset) {
        return key(buffer).toString(charset);
    }

    public static String keyString(final ByteBuf buffer) {
        return keyString(buffer, CharsetUtil.UTF_8);
    }

    public static long cas(final ByteBuf buffer) {
        return MessageUtil.getCas(buffer);
    }

    public static short partition(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static long bySeqno(final ByteBuf buffer) {
        return buffer.getLong(MessageUtil.HEADER_SIZE);
    }

    public static long revisionSeqno(final ByteBuf buffer) {
        return buffer.getLong(MessageUtil.HEADER_SIZE + 8);
    }

    public static String toString(final ByteBuf buffer) {
        return "DeletionMessage [key: \"" + keyString(buffer) + "\", vbid: " + partition(buffer) + ", cas: "
                + cas(buffer) + ", bySeqno: " + bySeqno(buffer) + ", revSeqno: " + revisionSeqno(buffer) + "]";
    }

}

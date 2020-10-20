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
        final byte magic = buffer.getByte(0);
        return buffer.getByte(1) == DCP_DELETION_OPCODE
                && (magic == MessageUtil.MAGIC_REQ || magic == MessageUtil.MAGIC_REQ_FLEX);
    }

    public static int cid(final ByteBuf buffer) {
        return MessageUtil.getCid(buffer);
    }

    public static ByteBuf key(final ByteBuf buffer, boolean isCollectionEnabled) {
        return MessageUtil.getKey(buffer, isCollectionEnabled);
    }

    public static String keyString(final ByteBuf buffer, Charset charset, boolean isCollectionEnabled) {
        return key(buffer, isCollectionEnabled).toString(charset);
    }

    public static String keyString(final ByteBuf buffer, boolean isCollectionEnabled) {
        return keyString(buffer, CharsetUtil.UTF_8, isCollectionEnabled);
    }

    public static long cas(final ByteBuf buffer) {
        return MessageUtil.getCas(buffer);
    }

    public static short partition(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static long bySeqno(final ByteBuf buffer) {
        return buffer.getLong(MessageUtil.getHeaderSize(buffer));
    }

    public static long revisionSeqno(final ByteBuf buffer) {
        return buffer.getLong(MessageUtil.getHeaderSize(buffer) + 8);
    }

    public static String toString(final ByteBuf buffer, boolean isCollectionEnabled) {
        return "DeletionMessage [key: \"" + keyString(buffer, isCollectionEnabled) + "\", vbid: " + partition(buffer)
                + ", cas: " + cas(buffer) + ", bySeqno: " + bySeqno(buffer) + ", revSeqno: " + revisionSeqno(buffer)
                + "]";
    }

}

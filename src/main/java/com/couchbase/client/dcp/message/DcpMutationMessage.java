/*
 * Copyright (c) 2016-2021 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_MUTATION_OPCODE;

import com.couchbase.client.dcp.util.CollectionsUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class DcpMutationMessage extends DcpDataMessage {

    private DcpMutationMessage() {
        throw new AssertionError("do not instantiate");
    }

    public static boolean is(final ByteBuf buffer) {
        final byte magic = buffer.getByte(0);
        return buffer.getByte(1) == DCP_MUTATION_OPCODE
                && (magic == MessageUtil.MAGIC_REQ || magic == MessageUtil.MAGIC_REQ_FLEX);
    }

    public static ByteBuf content(final ByteBuf buffer) {
        return MessageUtil.getContent(buffer);
    }

    public static byte[] contentBytes(final ByteBuf buffer) {
        byte[] bytes = new byte[buffer.readableBytes()];
        content(buffer).getBytes(0, bytes);
        return bytes;
    }

    public static int flags(final ByteBuf buffer) {
        return buffer.getInt(MessageUtil.getHeaderSize(buffer) + 16);
    }

    public static int expiry(final ByteBuf buffer) {
        return buffer.getInt(MessageUtil.getHeaderSize(buffer) + 20);
    }

    public static int lockTime(final ByteBuf buffer) {
        return buffer.getInt(MessageUtil.getHeaderSize(buffer) + 24);
    }

    public static String toString(final ByteBuf buffer, boolean collections) {
        return "MutationMessage [key: \"" + keyString(buffer, collections) + "\", "
                + (collections ? "cid: " + CollectionsUtil.displayCid(cid(buffer)) + ", " : "") + "vbid: "
                + partition(buffer) + ", cas: " + cas(buffer) + ", bySeqno: " + bySeqno(buffer) + ", revSeqno: "
                + revisionSeqno(buffer) + ", flags: " + flags(buffer) + ", expiry: " + expiry(buffer) + ", lockTime: "
                + lockTime(buffer) + ", clength: " + content(buffer).readableBytes() + "]";
    }
}

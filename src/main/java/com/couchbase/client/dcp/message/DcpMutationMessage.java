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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_MUTATION_OPCODE;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.util.CollectionsUtil;

import it.unimi.dsi.fastutil.ints.IntIntPair;

public class DcpMutationMessage extends DcpDataMessage {

    private DcpMutationMessage() {
        throw new AssertionError("do not instantiate");
    }

    public static boolean is(final ByteBuf buffer) {
        final byte magic = buffer.getByte(0);
        return buffer.getByte(1) == DCP_MUTATION_OPCODE
                && (magic == MessageUtil.MAGIC_REQ || magic == MessageUtil.MAGIC_REQ_FLEX);
    }

    public static IntIntPair keyContentLength(final ByteBuf buffer) {
        return MessageUtil.getKeyContentLength(buffer);
    }

    public static ByteBuf content(final ByteBuf buffer) {
        return MessageUtil.getContent(buffer);
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

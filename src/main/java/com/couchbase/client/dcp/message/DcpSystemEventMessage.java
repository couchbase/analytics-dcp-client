/*
 * Copyright (c) 2020 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class DcpSystemEventMessage {
    public enum Type {
        COLLECTION_CREATED,
        COLLECTION_DROPPED,
        COLLECTION_FLUSHED,
        SCOPE_CREATED,
        SCOPE_DROPPED,
        COLLECTION_CHANGED,
        UNKNOWN
    }

    private DcpSystemEventMessage() {
        throw new AssertionError("not instantiable");
    }

    public static boolean is(ByteBuf event) {
        return event.getByte(0) == MessageUtil.MAGIC_REQ && event.getByte(1) == MessageUtil.DCP_SYSTEM_EVENT_OPCODE;
    }

    public static short vbucket(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static long seqno(ByteBuf event) {
        return MessageUtil.getExtras(event).readLong();
    }

    public static int version(final ByteBuf event) {
        return MessageUtil.getExtras(event).getUnsignedByte(12);
    }

    public static Type type(ByteBuf event) {
        final int type = typeId(event);
        try {
            return Type.values()[type];
        } catch (ArrayIndexOutOfBoundsException e) {
            return Type.UNKNOWN;
        }
    }

    private static int typeId(ByteBuf event) {
        return MessageUtil.getExtras(event).getInt(8);
    }

    public static String toString(ByteBuf buffer) {
        return "DcpSystemEventMessage [vbucket: " + vbucket(buffer) + ", seqno: " + seqno(buffer) + ", type: "
                + type(buffer) + "(0x" + Integer.toHexString(typeId(buffer)) + "), version: " + version(buffer) + "]";
    }
}

/*
 * Copyright (c) 2020 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class DcpOsoSnapshotMarkerMessage {
    private static final int FLAG_BEGIN = 1;
    private static final int FLAG_END = 2;

    private DcpOsoSnapshotMarkerMessage() {
        throw new AssertionError("not instantiable");
    }

    public static boolean is(ByteBuf event) {
        return event.getByte(0) == MessageUtil.MAGIC_REQ
                && event.getByte(1) == MessageUtil.DCP_OSO_SNAPSHOT_MARKER_OPCODE;
    }

    public static short vbucket(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static int flags(ByteBuf event) {
        return MessageUtil.getExtras(event).readInt();
    }

    public static boolean begin(ByteBuf event) {
        return (flags(event) & FLAG_BEGIN) != 0;
    }

    public static boolean end(ByteBuf event) {
        return (flags(event) & FLAG_END) != 0;
    }

    public static String toString(final ByteBuf buffer) {
        return "DcpOsoSnapshotMarker [vbucket: " + vbucket(buffer) + ", flags: " + flags(buffer) + ", begin: "
                + begin(buffer) + ", end: " + end(buffer) + "]";
    }

}

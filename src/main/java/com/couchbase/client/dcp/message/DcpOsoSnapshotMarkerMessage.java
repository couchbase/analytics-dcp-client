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

import java.util.ArrayList;
import java.util.Collection;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

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

    public static void setMaxSeqNo(long maxSeqNo, final ByteBuf buffer) {
        // the analytics DCP client stashes the max sequence number observed in the oso snapshot in the unused CAS field
        MessageUtil.setCas(maxSeqNo, buffer);
    }

    public static long maxSeqNo(final ByteBuf buffer) {
        // the analytics DCP client stashes the max sequence number observed in the oso snapshot in the unused CAS field
        return MessageUtil.getCas(buffer);
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

    public static String humanizeFlags(ByteBuf event) {
        int flags = flags(event);
        Collection<String> setFlags = new ArrayList<>();
        if ((flags & FLAG_BEGIN) != 0) {
            flags &= ~FLAG_BEGIN;
            setFlags.add("BEGIN");
        }
        if ((flags & FLAG_END) != 0) {
            flags &= ~FLAG_END;
            setFlags.add("END");
        }
        if (flags != 0) {
            setFlags.add("<UNKNOWN FLAG(S): 0x" + Integer.toUnsignedString(flags, 16) + ">");
        }
        return setFlags.toString();
    }

    public static String toString(final ByteBuf buffer) {
        return "DcpOsoSnapshotMarker [vbucket: " + vbucket(buffer) + ", flags: " + humanizeFlags(buffer) + "]";
    }
}

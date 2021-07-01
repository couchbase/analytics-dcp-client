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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_SNAPSHOT_MARKER_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpSnapshotMarkerRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_SNAPSHOT_MARKER_OPCODE;
    }

    public static int flags(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getInt(16);
    }

    /**
     * Check if {@link SnapshotMarkerFlags#MEMORY} flag set for snapshot marker.
     */
    public static boolean memory(final ByteBuf buffer) {
        return SnapshotMarkerFlags.MEMORY.isSet(flags(buffer));
    }

    /**
     * Check if {@link SnapshotMarkerFlags#DISK} flag set for snapshot marker.
     */
    public static boolean disk(final ByteBuf buffer) {
        return SnapshotMarkerFlags.DISK.isSet(flags(buffer));
    }

    /**
     * Check if {@link SnapshotMarkerFlags#CHECKPOINT} flag set for snapshot marker.
     */
    public static boolean checkpoint(final ByteBuf buffer) {
        return SnapshotMarkerFlags.CHECKPOINT.isSet(flags(buffer));
    }

    /**
     * Check if {@link SnapshotMarkerFlags#ACK} flag set for snapshot marker.
     */
    public static boolean ack(final ByteBuf buffer) {
        return SnapshotMarkerFlags.ACK.isSet(flags(buffer));
    }

    public static long startSeqno(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getLong(0);

    }

    public static long endSeqno(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getLong(8);
    }

    public static String toString(final ByteBuf buffer) {
        return "SnapshotMarker [vbid: " + partition(buffer) + ", flags: " + String.format("0x%02x", flags(buffer))
                + ", start: " + startSeqno(buffer) + ", end: " + endSeqno(buffer) + "]";
    }

    public static short partition(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }
}

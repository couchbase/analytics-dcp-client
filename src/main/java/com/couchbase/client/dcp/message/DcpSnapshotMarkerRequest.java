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

import java.util.OptionalLong;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

public enum DcpSnapshotMarkerRequest {
    ;
    public static long startSeqno(ByteBuf buffer) {
        return handler(buffer).startSeqno(buffer);
    }

    public static long endSeqno(ByteBuf buffer) {
        return handler(buffer).endSeqno(buffer);
    }

    static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_SNAPSHOT_MARKER_OPCODE;
    }

    public static VersionSpecificHandler handler(final ByteBuf buffer) {
        if (MessageUtil.getExtras(buffer).capacity() > 1) {
            return V1.INSTANCE;
        } else if (MessageUtil.getExtras(buffer).getByte(0) == 0x02) {
            return V2_2.INSTANCE;
        } else {
            throw new IllegalArgumentException("Unknown DCP snapshot marker version");
        }
    }

    public interface VersionSpecificHandler {
        default short partition(final ByteBuf buffer) {
            return MessageUtil.getVbucket(buffer);
        }

        int flags(ByteBuf buffer);

        /**
         * Check if {@link SnapshotMarkerFlags#MEMORY} flag set for snapshot marker.
         */
        default boolean memory(final ByteBuf buffer) {
            return SnapshotMarkerFlags.MEMORY.isSet(flags(buffer));
        }

        /**
         * Check if {@link SnapshotMarkerFlags#DISK} flag set for snapshot marker.
         */
        default boolean disk(final ByteBuf buffer) {
            return SnapshotMarkerFlags.DISK.isSet(flags(buffer));
        }

        /**
         * Check if {@link SnapshotMarkerFlags#CHECKPOINT} flag set for snapshot marker.
         */
        default boolean checkpoint(final ByteBuf buffer) {
            return SnapshotMarkerFlags.CHECKPOINT.isSet(flags(buffer));
        }

        /**
         * Check if {@link SnapshotMarkerFlags#ACK} flag set for snapshot marker.
         */
        default boolean ack(final ByteBuf buffer) {
            return SnapshotMarkerFlags.ACK.isSet(flags(buffer));
        }

        long startSeqno(ByteBuf buffer);

        long endSeqno(ByteBuf buffer);

        OptionalLong purgeSeqno(ByteBuf buffer);

        String toString(final ByteBuf buffer);
    }

    private static class V1 implements VersionSpecificHandler {

        static final VersionSpecificHandler INSTANCE = new V1();

        public int flags(final ByteBuf buffer) {
            return MessageUtil.getExtras(buffer).getInt(16);
        }

        @Override
        public long startSeqno(final ByteBuf buffer) {
            return MessageUtil.getExtras(buffer).getLong(0);

        }

        @Override
        public long endSeqno(final ByteBuf buffer) {
            return MessageUtil.getExtras(buffer).getLong(8);
        }

        @Override
        public OptionalLong purgeSeqno(ByteBuf buffer) {
            return OptionalLong.empty();
        }

        @Override
        public String toString(final ByteBuf buffer) {
            return "SnapshotMarker [vbid: " + partition(buffer) + ", flags: " + String.format("0x%02x", flags(buffer))
                    + ", start: " + startSeqno(buffer) + ", end: " + endSeqno(buffer) + "]";
        }
    }

    private static class V2_2 implements VersionSpecificHandler {

        static final VersionSpecificHandler INSTANCE = new V2_2();

        /*
         * 0:  8b Start Seqno
         * 8:  8b End Seqno
         * 16: 4b Snapshot Type
         * 20: 8b Max Visible Seqno
         * 28: 8b High Completed Seqno
         * 36: 8b Purge Seqno
         */
        @Override
        public int flags(final ByteBuf buffer) {
            return MessageUtil.getContent(buffer).getInt(16);
        }

        @Override
        public long startSeqno(final ByteBuf buffer) {
            return MessageUtil.getContent(buffer).getLong(0);

        }

        @Override
        public long endSeqno(final ByteBuf buffer) {
            return MessageUtil.getContent(buffer).getLong(8);
        }

        @Override
        public OptionalLong purgeSeqno(final ByteBuf buffer) {
            return OptionalLong.of(MessageUtil.getContent(buffer).getLong(36));
        }

        public String toString(final ByteBuf buffer) {
            return "SnapshotMarker(v2.2) [vbid: " + partition(buffer) + ", flags: "
                    + String.format("0x%02x", flags(buffer)) + ", start: " + startSeqno(buffer) + ", end: "
                    + endSeqno(buffer) + ", purge: " + purgeSeqno(buffer) + "]";
        }
    }
}

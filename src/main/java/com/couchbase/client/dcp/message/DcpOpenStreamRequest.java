/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_REQUEST_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

public enum DcpOpenStreamRequest {
    ;

    /**
     * If the given buffer is a {@link DcpOpenStreamRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_STREAM_REQUEST_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     *
     * Initializes the complete extras needed with 0 and can be overridden through the setters available.
     * If no setters are used this message is effectively a backfill for the given vbucket.
     */
    public static void init(final ByteBuf buffer, short vbucket) {
        MessageUtil.initRequest(DCP_STREAM_REQUEST_OPCODE, buffer);

        MessageUtil.setVbucket(vbucket, buffer);
        MessageUtil.setExtras(
                Unpooled.buffer(48).writeInt(0) // flags
                        .writeInt(0) // reserved
                        .writeLong(0) // start sequence number
                        .writeLong(0) // end sequence number
                        .writeLong(0) // vbucket uuid
                        .writeLong(0) // snapshot start sequence number
                        .writeLong(0), // snapshot end sequence number
                buffer);
    }

    public static void startSeqno(final ByteBuf buffer, long seqnoStart) {
        MessageUtil.getExtras(buffer).setLong(8, seqnoStart);
    }

    public static void endSeqno(final ByteBuf buffer, long seqnoEnd) {
        MessageUtil.getExtras(buffer).setLong(16, seqnoEnd);
    }

    public static void vbuuid(final ByteBuf buffer, long uuid) {
        MessageUtil.getExtras(buffer).setLong(24, uuid);
    }

    public static void snapshotStartSeqno(final ByteBuf buffer, long snapshotSeqnoStart) {
        MessageUtil.getExtras(buffer).setLong(32, snapshotSeqnoStart);

    }

    public static void snapshotEndSeqno(final ByteBuf buffer, long snapshotSeqnoEnd) {
        MessageUtil.getExtras(buffer).setLong(40, snapshotSeqnoEnd);

    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        MessageUtil.setOpaque(opaque, buffer);
    }

    public static void setValue(final ByteBuf value, final ByteBuf buffer) {
        MessageUtil.setContent(value, buffer);
    }

    public static int flags(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getInt(0);
    }

    public static void flags(final ByteBuf buffer, int flags) {
        MessageUtil.getExtras(buffer).setInt(0, flags);
    }

    /**
     * Set {@link StreamFlags#TAKEOVER} flag for the stream.
     */
    public static void takeover(final ByteBuf buffer) {
        flags(buffer, flags(buffer) | StreamFlags.TAKEOVER.value());
    }

    /**
     * Set {@link StreamFlags#DISK_ONLY} flag for the stream.
     */
    public static void diskOnly(final ByteBuf buffer) {
        flags(buffer, flags(buffer) | StreamFlags.DISK_ONLY.value());
    }

    /**
     * Set {@link StreamFlags#LATEST} flag for the stream.
     */
    public static void latest(final ByteBuf buffer) {
        flags(buffer, flags(buffer) | StreamFlags.LATEST.value());
    }

    /**
     * Set {@link StreamFlags#NO_VALUE} flag for the stream.
     */
    public static void noValue(final ByteBuf buffer) {
        flags(buffer, flags(buffer) | StreamFlags.NO_VALUE.value());
    }

    /**
     * Set {@link StreamFlags#ACTIVE_VB_ONLY} flag for the stream.
     */
    public static void activeVbucketOnly(final ByteBuf buffer) {
        flags(buffer, flags(buffer) | StreamFlags.ACTIVE_VB_ONLY.value());
    }
}

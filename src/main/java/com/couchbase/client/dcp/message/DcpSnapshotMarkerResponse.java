/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_SNAPSHOT_MARKER_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpSnapshotMarkerResponse {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_SNAPSHOT_MARKER_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initResponse(DCP_SNAPSHOT_MARKER_OPCODE, buffer);
    }
}

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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_ADD_STREAM_OPCODE;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

/**
 * Sent to the consumer to tell the consumer to initiate a stream request with the producer
 */
public enum DcpAddStreamRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_ADD_STREAM_OPCODE;
    }

    public static int flags(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getInt(0);
    }

    /**
     * Check if {@link StreamFlags#TAKEOVER} flag requested for the stream.
     */
    public static boolean takeover(final ByteBuf buffer) {
        return StreamFlags.TAKEOVER.isSet(flags(buffer));
    }

    /**
     * Check if {@link StreamFlags#DISK_ONLY} flag requested for the stream.
     */
    public static boolean diskOnly(final ByteBuf buffer) {
        return StreamFlags.DISK_ONLY.isSet(flags(buffer));
    }

    /**
     * Check if {@link StreamFlags#LATEST} flag requested for the stream.
     */
    public static boolean latest(final ByteBuf buffer) {
        return StreamFlags.LATEST.isSet(flags(buffer));
    }

    /**
     * Check if {@link StreamFlags#ACTIVE_VB_ONLY} flag requested for the stream.
     */
    public static boolean activeVbucketOnly(final ByteBuf buffer) {
        return StreamFlags.ACTIVE_VB_ONLY.isSet(flags(buffer));
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(DCP_ADD_STREAM_OPCODE, buffer);
        flags(buffer, 0);
    }

    public static void flags(final ByteBuf buffer, int flags) {
        ByteBuf extras = Unpooled.buffer(4);
        MessageUtil.setExtras(extras.writeInt(flags), buffer);
        extras.release();
    }
}

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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_COLLECTIONS_MANIFEST_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpGetCollectionsManifestRequest {
    ;

    /**
     * If the given buffer is a {@link DcpGetCollectionsManifestRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_COLLECTIONS_MANIFEST_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     *
     * Initializes the complete extras needed with 0 and can be overridden through the setters available.
     * If no setters are used this message is effectively a backfill for the given vbucket.
     */
    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(DCP_COLLECTIONS_MANIFEST_OPCODE, buffer);
    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        MessageUtil.setOpaque(opaque, buffer);
    }
}

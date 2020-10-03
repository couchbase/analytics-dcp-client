/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.GET_COLLECTIONS_MANIFEST_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpGetCollectionsManifestResponse {
    ;

    /**
     * If the given buffer is a {@link DcpGetCollectionsManifestResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == GET_COLLECTIONS_MANIFEST_OPCODE;
    }

}

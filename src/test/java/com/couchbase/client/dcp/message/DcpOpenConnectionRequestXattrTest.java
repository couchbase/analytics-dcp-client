/*
 * Copyright 2016-2024 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

/**
 * Verifies that the DCP open-connection request only sets {@link DcpOpenConnectionRequest#FLAG_INCLUDE_XATTRS}
 * when xattr inclusion is requested. This is the load-bearing behavior of the DCP_INGEST_XATTRS flag:
 * DcpConnectHandler#openConnection passes {@code includeXattrs ? FLAG_INCLUDE_XATTRS : 0} to
 * {@link DcpOpenConnectionRequest#init(ByteBuf, int)}. When the xattr flag is not requested, mutations from KV
 * do not include xattrs.
 */
public class DcpOpenConnectionRequestXattrTest {

    private static final int PRODUCER = 0x01;

    /** Reads back the open-connection flags int (extras layout: int seqno(0), int flags(4)). */
    private static int openFlags(int extraFlags) {
        ByteBuf buffer = Unpooled.buffer();
        try {
            DcpOpenConnectionRequest.init(buffer, extraFlags);
            return MessageUtil.getExtras(buffer).getInt(Integer.BYTES);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void xattrsRequestedWhenFlagSet() {
        int flags = openFlags(DcpOpenConnectionRequest.FLAG_INCLUDE_XATTRS);
        assertEquals(PRODUCER | DcpOpenConnectionRequest.FLAG_INCLUDE_XATTRS, flags);
        assertNotEquals(0, flags & DcpOpenConnectionRequest.FLAG_INCLUDE_XATTRS);
    }

    @Test
    public void xattrsNotRequestedWhenFlagClear() {
        int flags = openFlags(0);
        assertEquals(PRODUCER, flags);
        assertEquals(0, flags & DcpOpenConnectionRequest.FLAG_INCLUDE_XATTRS);
    }
}

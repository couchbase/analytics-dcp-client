/*
 * Copyright 2026-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.transport.netty;

import static com.couchbase.client.dcp.transport.netty.Hello.HELO_COLLECTIONS;
import static com.couchbase.client.dcp.transport.netty.Hello.HELO_DCP_SKIP_DELETES_IN_INITIAL_BACKFILL;
import static com.couchbase.client.dcp.transport.netty.Hello.HELO_XERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.dcp.message.DcpOpenConnectionRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.ShortSortedBitSet;

import it.unimi.dsi.fastutil.shorts.ShortSet;

/**
 * A server that does not know {@link DcpOpenConnectionRequest#FLAG_SKIP_DELETES_IN_INITIAL_BACKFILL} fails the
 * DCP open with EINVAL, so the flag may only be sent once HELO shows the server supports it.
 */
public class SkipDeletesInInitialBackfillTest {

    private static ShortSet heloResponseFeatures(short... features) {
        ByteBuf response = Unpooled.buffer();
        try {
            MessageUtil.initRequest(MessageUtil.HELO_OPCODE, response);
            response.setByte(0, MessageUtil.MAGIC_RES);
            MessageUtil.setContent(Unpooled.copyShort(features), response);
            return Hello.features(response);
        } finally {
            response.release();
        }
    }

    @Test
    public void parsesHeloResponse() {
        assertEquals(
                new ShortSortedBitSet(
                        new short[] { HELO_XERROR, HELO_COLLECTIONS, HELO_DCP_SKIP_DELETES_IN_INITIAL_BACKFILL }),
                heloResponseFeatures(HELO_XERROR, HELO_COLLECTIONS, HELO_DCP_SKIP_DELETES_IN_INITIAL_BACKFILL));
        assertTrue(heloResponseFeatures().isEmpty());
    }

    @Test
    public void supportedWhenServerAcknowledgesFeatureAndCollections() {
        assertTrue(DcpConnectHandler.supportsSkipDeletesInInitialBackfill(
                heloResponseFeatures(HELO_XERROR, HELO_COLLECTIONS, HELO_DCP_SKIP_DELETES_IN_INITIAL_BACKFILL)));
    }

    @Test
    public void unsupportedByOlderServer() {
        assertFalse(DcpConnectHandler
                .supportsSkipDeletesInInitialBackfill(heloResponseFeatures(HELO_XERROR, HELO_COLLECTIONS)));
    }

    @Test
    public void unsupportedWithoutCollections() {
        assertFalse(DcpConnectHandler.supportsSkipDeletesInInitialBackfill(
                heloResponseFeatures(HELO_XERROR, HELO_DCP_SKIP_DELETES_IN_INITIAL_BACKFILL)));
    }

    @Test
    public void openFlagIsSent() {
        ByteBuf request = Unpooled.buffer();
        try {
            DcpOpenConnectionRequest.init(request, DcpOpenConnectionRequest.FLAG_SKIP_DELETES_IN_INITIAL_BACKFILL);
            assertEquals(0x201, MessageUtil.getExtras(request).getInt(Integer.BYTES));
        } finally {
            request.release();
        }
    }
}

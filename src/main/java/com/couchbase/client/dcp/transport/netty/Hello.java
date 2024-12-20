/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.dcp.message.MessageUtil;

public class Hello {
    public static final short HELO_DATATYPE = 0x01;
    public static final short HELO_TLS = 0x02;
    public static final short HELO_TCPNODELAY = 0x03;
    public static final short HELO_MUTATIONSEQ = 0x04;
    public static final short HELO_TCPDELAY = 0x05;
    public static final short HELO_XATTR = 0x06;
    public static final short HELO_XERROR = 0x07;
    public static final short HELO_SELECT = 0x08;
    public static final short HELO_SNAPPY = 0x0a;
    public static final short HELO_COLLECTIONS = 0x12;

    private Hello() {
    }

    public static void init(ByteBuf buffer, ByteBuf connectionName, short... flags) {
        MessageUtil.initRequest(MessageUtil.HELO_OPCODE, buffer);
        MessageUtil.setKey(connectionName, buffer);
        MessageUtil.setContent(Unpooled.copyShort(flags), buffer);
    }
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

public class Hello {
    public static final short DATATYPE = 0x01;
    public static final short TLS = 0x02;
    public static final short TCPNODELAY = 0x03;
    public static final short MUTATIONSEQ = 0x04;
    public static final short TCPDELAY = 0x05;
    public static final short XATTR = 0x06;
    public static final short XERROR = 0x07;
    public static final short SELECT = 0x08;
    public static final short COLLECTIONS = 0x12;

    private Hello() {
    }

    public static void init(ByteBuf buffer, ByteBuf connectionName) {
        MessageUtil.initRequest(MessageUtil.HELO_OPCODE, buffer);
        MessageUtil.setKey(connectionName, buffer);
        MessageUtil.setContent(Unpooled.copyShort(XERROR, SELECT, COLLECTIONS), buffer);
    }
}

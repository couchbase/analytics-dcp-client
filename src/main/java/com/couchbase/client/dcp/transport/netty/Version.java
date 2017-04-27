/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class Version {
    private Version() {
    }

    public static void init(ByteBuf buffer) {
        MessageUtil.initRequest(MessageUtil.VERSION_OPCODE, buffer);
    }
}

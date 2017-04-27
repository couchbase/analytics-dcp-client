/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

public class BucketSelectionRequest {
    private BucketSelectionRequest() {

    }

    public static void init(ByteBuf buffer, String bucket) {
        MessageUtil.initRequest(MessageUtil.SELECT_BUCKET_OPCODE, buffer);
        ByteBuf key = Unpooled.copiedBuffer(bucket, CharsetUtil.UTF_8);
        MessageUtil.setKey(key, buffer);
        key.release();
    }

}

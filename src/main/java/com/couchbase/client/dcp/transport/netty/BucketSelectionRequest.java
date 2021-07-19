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

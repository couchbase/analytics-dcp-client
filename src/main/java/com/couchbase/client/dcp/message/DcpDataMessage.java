/*
 * Copyright 2021-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.message;

import java.nio.charset.Charset;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;

@SuppressWarnings("squid:S1610")
public abstract class DcpDataMessage {

    protected DcpDataMessage() {
        throw new AssertionError("do not instantiate");
    }

    public static int cid(final ByteBuf buffer) {
        return MessageUtil.getCid(buffer);
    }

    public static ByteBuf key(final ByteBuf buffer, boolean isCollectionEnabled) {
        return MessageUtil.getKey(buffer, isCollectionEnabled);
    }

    public static String keyString(final ByteBuf buffer, Charset charset, boolean isCollectionEnabled) {
        return key(buffer, isCollectionEnabled).toString(charset);
    }

    public static String keyString(final ByteBuf buffer, boolean isCollectionEnabled) {
        return keyString(buffer, CharsetUtil.UTF_8, isCollectionEnabled);
    }

    public static short partition(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static long cas(final ByteBuf buffer) {
        return MessageUtil.getCas(buffer);
    }

    public static long bySeqno(final ByteBuf buffer) {
        return buffer.getLong(MessageUtil.getHeaderSize(buffer));
    }

    public static long revisionSeqno(final ByteBuf buffer) {
        return buffer.getLong(MessageUtil.getHeaderSize(buffer) + 8);
    }
}

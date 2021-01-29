/*
 * Copyright (c) 2021 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import java.nio.charset.Charset;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

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

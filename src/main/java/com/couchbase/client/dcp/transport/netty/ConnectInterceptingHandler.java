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

import java.net.SocketAddress;

import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelOutboundHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.core.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.core.deps.io.netty.util.concurrent.GenericFutureListener;

/**
 * Helper class which intercepts the connect promise and stores it for later reuse.
 *
 * Subclasses are expected that once their stuff is complete take the {@link #originalPromise()}
 * and complete or fail it accordingly, otherwise the connect chain will be stuck forever and
 * maybe time out depending on the channel config.
 */
abstract class ConnectInterceptingHandler<T> extends SimpleChannelInboundHandler<T> implements ChannelOutboundHandler {

    /**
     * The original connect promise which is intercepted and then completed/failed after the
     * authentication procedure.
     */
    private ChannelPromise originalPromise;

    /**
     * Returns the intercepted original connect promise for completion or failure.
     */
    ChannelPromise originalPromise() {
        return originalPromise;
    }

    /**
     * Intercept the connect phase and store the original promise.
     */
    @Override
    public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
            final SocketAddress localAddress, final ChannelPromise promise) throws Exception {
        originalPromise = promise;
        ChannelPromise inboundPromise = ctx.newPromise();
        inboundPromise.addListener(new GenericFutureListener<Future<Void>>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (!future.isSuccess() && !originalPromise.isDone()) {
                    originalPromise.setFailure(future.cause());
                }
            }
        });
        ctx.connect(remoteAddress, localAddress, inboundPromise);
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        ctx.bind(localAddress, promise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.disconnect(promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.close(promise);
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.deregister(promise);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        ctx.read();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ctx.read();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (originalPromise != null) {
            originalPromise.setFailure(cause);
        }
        ctx.fireExceptionCaught(cause);
    }
}

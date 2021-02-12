/*
 * Copyright (c) 2018 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.net.SocketAddress;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.dcp.error.AuthorizationException;
import com.couchbase.client.dcp.error.BucketNotFoundException;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.ChannelOutboundHandler;
import com.couchbase.client.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.handler.codec.base64.Base64;
import com.couchbase.client.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpHeaders;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;

public class RequestConfigHandler extends SimpleChannelInboundHandler<HttpResponse> implements ChannelOutboundHandler {
    private static final Logger LOGGER = LogManager.getLogger();
    private final String bucket;
    private final String uuid;
    private final String username;
    private final String password;
    private final MutableObject<CouchbaseBucketConfig> config;
    private final MutableObject<Throwable> failure;
    /**
     * The original connect promise which is intercepted and then completed/failed after the
     * authentication procedure.
     */
    private ChannelPromise originalPromise;

    RequestConfigHandler(final String bucket, final String username, final String password, final String uuid,
            MutableObject<CouchbaseBucketConfig> config, MutableObject<Throwable> failure) {
        this.bucket = bucket;
        this.uuid = uuid;
        this.username = username;
        this.password = password;
        this.config = config;
        this.failure = failure;
    }

    /**
     * Returns the intercepted original connect promise for completion or failure.
     */
    ChannelPromise originalPromise() {
        return originalPromise;
    }

    /**
     * Once the channel is active, start to send the HTTP request to begin chunking.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        String terseUri = "/pools/default/b/" + URLEncodedUtils.formatSegments(bucket)
                + (uuid.isEmpty() ? uuid : ('?' + Conductor.KEY_BUCKET_UUID + uuid));
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, terseUri);
        request.headers().add(HttpHeaders.Names.ACCEPT, "application/json");
        request.headers().add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        addHttpBasicAuth(ctx, request);
        ctx.writeAndFlush(request);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final HttpResponse msg) throws Exception {
        int statusCode = msg.getStatus().code();
        LOGGER.debug("Status code " + statusCode);
        if (statusCode == 200) {
            ctx.pipeline().remove(this);
            originalPromise().setSuccess();
            ctx.fireChannelActive();
        } else {
            CouchbaseException exception;
            switch (statusCode) {
                case 401:
                    exception = new AuthorizationException(
                            "Unauthorized - Incorrect credentials or bucket " + bucket + " does not exist");
                    break;
                case 404:
                    exception = new BucketNotFoundException("Bucket " + bucket + " does not exist");
                    break;
                default:
                    exception = new CouchbaseException("Unknown error code during connect: " + msg.getStatus());

            }
            synchronized (config) {
                if (failure.getValue() == null) {
                    failure.setValue(exception);
                    config.notifyAll();
                } else {
                    LOGGER.log(Level.WARN, "Subsequent failure trying to get bucket configuration", exception);
                }
            }
            if (!originalPromise().isDone()) {
                originalPromise().setFailure(exception);
            }
        }
    }

    /**
     * Helper method to add authentication credentials to the config stream request.
     */
    private void addHttpBasicAuth(final ChannelHandlerContext ctx, final HttpRequest request) {
        if (username == null || username.isEmpty()) {
            return;
        }
        final String pw = password == null ? "" : password;
        ByteBuf raw = ctx.alloc().buffer(username.length() + pw.length() + 1);
        raw.writeBytes((username + ":" + pw).getBytes(CharsetUtil.UTF_8));
        ByteBuf encoded = Base64.encode(raw, false);
        request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CharsetUtil.UTF_8));
        encoded.release();
        raw.release();
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
        ctx.write(msg, promise);
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

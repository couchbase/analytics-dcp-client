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
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.base64.Base64;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaders;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.dcp.error.AuthorizationException;
import com.couchbase.client.dcp.error.BucketNotFoundException;

/**
 * This handler intercepts the bootstrap of the config stream, sending the initial request
 * and checking the response for potential errors.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
class StartStreamHandler extends ConnectInterceptingHandler<HttpResponse> {

    private final String bucket;
    private final String username;
    private final String password;

    StartStreamHandler(final String bucket, final String username, final String password) {
        this.bucket = bucket;
        this.username = username;
        this.password = password;
    }

    /**
     * Once the channel is active, start to send the HTTP request to begin chunking.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        String terseUri = "/pools/default/bs/" + bucket;
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, terseUri);
        request.headers().add(HttpHeaders.Names.ACCEPT, "application/json");
        addHttpBasicAuth(ctx, request);
        ctx.writeAndFlush(request);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final HttpResponse msg) throws Exception {
        int statusCode = msg.getStatus().code();
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
            originalPromise().setFailure(exception);
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
}

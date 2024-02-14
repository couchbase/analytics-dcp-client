/*
 * Copyright 2018-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.conductor;

import static org.apache.hyracks.util.NetworkUtil.encodeIPv6LiteralHost;

import java.net.InetSocketAddress;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpObject;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.dcp.config.ClientEnvironment;

public class NonStreamingConfigHandler extends SimpleChannelInboundHandler<HttpObject> {
    public static final Logger LOGGER = LogManager.getLogger();

    /**
     * Hostname used to replace $HOST parts in the config when used against localhost.
     */
    private final InetSocketAddress address;

    /**
     * The config stream where the configs are emitted into.
     */
    private final MutableObject<CouchbaseBucketConfig> config;
    private final MutableObject<Throwable> failure;

    /**
     * The current aggregated chunk of the JSON config.
     */
    private ByteBuf responseContent;

    /**
     * Creates a new config handler.
     *
     * @param address
     *            address of the remote server.
     * @param environment
     * @param config
     *            config stream where to send the configs.
     */
    NonStreamingConfigHandler(final InetSocketAddress address, ClientEnvironment environment,
            MutableObject<CouchbaseBucketConfig> config, MutableObject<Throwable> failure) {
        this.address = address;
        this.config = config;
        this.failure = failure;
    }

    /**
     * If we get a new content chunk, send it towards decoding.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        if (msg instanceof HttpContent) {
            synchronized (config) {
                if (failure.getValue() == null) {
                    HttpContent content = (HttpContent) msg;
                    responseContent.writeBytes(content.content());
                } else {
                    LOGGER.log(Level.DEBUG, "Already failed getting configurations");
                }
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        synchronized (config) {
            if (failure.getValue() == null) {
                String rawConfig = null;
                String hostAddress = address.getHostString();
                if (responseContent != null) {
                    rawConfig = responseContent.toString(CharsetUtil.UTF_8).replace("$HOST",
                            encodeIPv6LiteralHost(hostAddress));
                    LOGGER.log(Level.TRACE, "received config: {}", rawConfig);
                }
                if (rawConfig != null && !rawConfig.isEmpty()) {
                    try {
                        config.setValue((CouchbaseBucketConfig) BucketConfigParser.parse(rawConfig, null, hostAddress));
                    } catch (Exception e) {
                        LOGGER.error("failed to parse raw config: {}", rawConfig);
                        failure.setValue(e);
                    }
                } else {
                    failure.setValue(new CouchbaseException("Received raw config is " + rawConfig));
                }
                config.notifyAll();
            }
        }
        ctx.fireChannelInactive();
    }

    /**
     * Once the handler is added, initialize the response content buffer.
     */
    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        responseContent = ctx.alloc().buffer();
    }

    /**
     * Once the handler is removed, make sure the response content is released and freed.
     */
    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        if (responseContent != null && responseContent.refCnt() > 0) {
            responseContent.release();
            responseContent = null;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        synchronized (config) {
            if (failure.getValue() == null) {
                failure.setValue(cause);
                config.notifyAll();
            } else if (failure.getValue() != cause) {
                LOGGER.log(Level.WARN, "Subsequent failure trying to get bucket configuration", cause);
            }
        }
        ctx.close();
    }
}

/*
 * Copyright (c) 2018 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpObject;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

public class NonStreamingConfigHandler extends SimpleChannelInboundHandler<HttpObject> {
    public static final Logger LOGGER = LogManager.getLogger();

    /**
     * Hostname used to replace $HOST parts in the config when used against localhost.
     */
    private final String hostname;

    /**
     * The config stream where the configs are emitted into.
     */
    private final MutableObject<CouchbaseBucketConfig> config;
    private final MutableObject<Throwable> failure;
    private final ClientEnvironment environment;

    /**
     * The current aggregated chunk of the JSON config.
     */
    private ByteBuf responseContent;

    /**
     * Creates a new config handler.
     *
     * @param hostname
     *            hostname of the remote server.
     * @param configStream
     *            config stream where to send the configs.
     * @param environment
     *            the environment.
     */
    NonStreamingConfigHandler(final String hostname, ClientEnvironment environment,
            MutableObject<CouchbaseBucketConfig> config, MutableObject<Throwable> failure) {
        this.hostname = hostname;
        this.config = config;
        this.failure = failure;
        this.environment = environment;
    }

    /**
     * If we get a new content chunk, send it towards decoding.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;
            responseContent.writeBytes(content.content());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String rawConfig = responseContent.toString(CharsetUtil.UTF_8).replace("$HOST", hostname);
        LOGGER.log(Level.DEBUG, "Received Config: {}", rawConfig);
        synchronized (config) {
            if (failure.getValue() == null) {
                try {
                    config.setValue((CouchbaseBucketConfig) BucketConfigParser.parse(rawConfig, environment));
                } catch (Exception e) {
                    failure.setValue(e);
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

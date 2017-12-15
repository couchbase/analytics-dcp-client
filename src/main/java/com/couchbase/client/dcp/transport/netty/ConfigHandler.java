/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.dcp.conductor.IConfigurable;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpObject;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

import rx.subjects.Subject;

/**
 * This handler is responsible to consume chunks of JSON configs via HTTP, aggregate them and once a complete
 * config is received send it into a {@link Subject} for external usage.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
class ConfigHandler extends SimpleChannelInboundHandler<HttpObject> {
    public static final Logger LOGGER = LogManager.getLogger();

    /**
     * Hostname used to replace $HOST parts in the config when used against localhost.
     */
    private final String hostname;

    /**
     * The config stream where the configs are emitted into.
     */
    private final IConfigurable configurable;
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
    ConfigHandler(final String hostname, IConfigurable configurable, ClientEnvironment environment) {
        this.hostname = hostname;
        this.configurable = configurable;
        this.environment = environment;
    }

    /**
     * If we get a new content chunk, send it towards decoding.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;
            if (decodeChunk(content.content())) {
                ctx.close();
            }
        }
    }

    /**
     * Helper method to decode and analyze the chunk.
     *
     * @param chunk
     *            the chunk to analyze.
     */
    private boolean decodeChunk(final ByteBuf chunk) {
        responseContent.writeBytes(chunk);
        String currentChunk = responseContent.toString(CharsetUtil.UTF_8);
        int separatorIndex = currentChunk.indexOf("\n\n\n\n");
        if (separatorIndex > 0) {
            String rawConfig = currentChunk.substring(0, separatorIndex).trim().replace("$HOST", hostname);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.log(Level.DEBUG, "Received Config: " + rawConfig);
            }
            try {
                configurable.configure((CouchbaseBucketConfig) BucketConfigParser.parse(rawConfig, environment));
            } catch (Exception e) {
                configurable.fail(e);
            }
            return true;
        }
        return false;
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
        configurable.fail(cause);
        ctx.close();
    }
}

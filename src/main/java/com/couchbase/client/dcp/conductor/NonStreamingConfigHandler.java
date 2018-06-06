/*
 * Copyright (c) 2018 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import static org.apache.hyracks.util.NetworkUtil.encodeIPv6LiteralHost;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpObject;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
     * @param configStream
     *            config stream where to send the configs.
     * @param address
     *            address of the remote server.
     * @param environment
     */
    NonStreamingConfigHandler(final InetSocketAddress address, ClientEnvironment environment,
            MutableObject<CouchbaseBucketConfig> config, MutableObject<Throwable> failure) {
        this.hostname = address.getHostString();
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
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        synchronized (config) {
            if (failure.getValue() == null) {
                String rawConfig = null;
                if (responseContent != null) {
                    rawConfig = fixupJVMCBC521(responseContent.toString(CharsetUtil.UTF_8)).replace("$HOST",
                            encodeIPv6LiteralHost(hostname));
                    LOGGER.log(Level.DEBUG, "Received Config: {}", rawConfig);
                }
                if (rawConfig != null && !rawConfig.isEmpty()) {
                    try {
                        config.setValue((CouchbaseBucketConfig) BucketConfigParser.parse(rawConfig, environment));
                    } catch (Exception e) {
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

    private String fixupJVMCBC521(String orig) throws IOException {
        try {
            JsonNode config = new ObjectMapper().readTree(orig);
            ArrayNode nodesExt = (ArrayNode) config.get("nodesExt");
            for (Iterator<JsonNode> iter = nodesExt.elements(); iter.hasNext();) {
                ObjectNode node = (ObjectNode) iter.next();
                if (!node.has("hostname")) {
                    node.put("hostname", "$HOST");
                }
            }
            return JSONUtil.convertNode(config);
        } catch (Exception e) {
            LOGGER.warn("ignoring exception trying to fixup JVMCBC-521", e);
            return orig;
        }
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

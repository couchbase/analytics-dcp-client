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

import java.net.InetSocketAddress;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import com.couchbase.client.core.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.core.deps.io.netty.handler.logging.LoggingHandler;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslHandler;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.SSLEngineFactory;
import com.couchbase.client.dcp.message.MessageUtil;

/**
 * Sets up the pipeline for the actual DCP communication channels.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpPipeline extends ChannelInitializer<Channel> {

    /**
     * The logger used.
     */
    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * The stateful environment.
     */
    private final ClientEnvironment environment;

    /**
     * The observable where all the control events are fed into for advanced handling up the stack.
     */
    private final ControlEventHandler controlHandler;
    private final SSLEngineFactory sslEngineFactory;
    private final DcpChannel dcpChannel;
    private final String host;
    private final int port;

    /**
     * Creates the pipeline.
     *
     * @param dcpChannel
     *
     * @param address
     *
     * @param environment
     *            the stateful environment.
     * @param controlHandler
     *            the control event observable.
     */
    public DcpPipeline(DcpChannel dcpChannel, String host, int port, final ClientEnvironment environment,
            final ControlEventHandler controlHandler) {
        this.dcpChannel = dcpChannel;
        this.host = host;
        this.port = port;
        this.environment = environment;
        this.controlHandler = controlHandler;
        if (environment.sslEnabled()) {
            this.sslEngineFactory = new SSLEngineFactory(environment);
        } else {
            this.sslEngineFactory = null;
        }
    }

    /**
     * Initializes the full pipeline with all handlers needed (some of them may remove themselves during
     * steady state, like auth and feature negotiation).
     */
    @Override
    protected void initChannel(final Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        if (environment.sslEnabled()) {
            pipeline.addLast(new SslHandler(sslEngineFactory.get()));
        }
        pipeline.addLast(
                new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, MessageUtil.BODY_LENGTH_OFFSET, 4, 12, 0, false));

        if (LOGGER.isTraceEnabled()) {
            pipeline.addLast(new LoggingHandler(LogLevel.TRACE));
        }

        Pair<String, String> pair =
                environment.credentialsProvider().get(InetSocketAddress.createUnresolved(host, port));
        if (pair != null && pair.getLeft() != null) {
            pipeline.addLast(new AuthHandler(pair.getLeft(), pair.getRight()));
        }
        pipeline.addLast(new DcpConnectHandler(environment, dcpChannel))
                .addLast(new DcpNegotiationHandler(environment.dcpControl())).addLast(new DcpMessageHandler(dcpChannel,
                        ch, environment, environment.dataEventHandler(), controlHandler));
    }
}

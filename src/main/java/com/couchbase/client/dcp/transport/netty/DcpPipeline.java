/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.SSLEngineFactory;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import com.couchbase.client.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.deps.io.netty.handler.logging.LoggingHandler;
import com.couchbase.client.deps.io.netty.handler.ssl.SslHandler;

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
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpPipeline.class);

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

        pipeline.addLast(new AuthHandler(environment.credentialsProvider(), host, port))
                .addLast(new DcpConnectHandler(environment))
                .addLast(new DcpNegotiationHandler(environment.dcpControl())).addLast(new DcpMessageHandler(dcpChannel,
                        ch, environment, environment.dataEventHandler(), controlHandler));
    }
}

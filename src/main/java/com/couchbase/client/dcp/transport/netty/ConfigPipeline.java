/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.conductor.IConfigurable;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.SSLEngineFactory;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpClientCodec;
import com.couchbase.client.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.deps.io.netty.handler.logging.LoggingHandler;
import com.couchbase.client.deps.io.netty.handler.ssl.SslHandler;

/**
 * Configures the pipeline for the HTTP config stream.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class ConfigPipeline extends ChannelInitializer<Channel> {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ConfigPipeline.class);

    /**
     * The stateful environment.
     */
    private final ClientEnvironment environment;

    /**
     * Hostname used to replace $HOST parts in the config when used against localhost.
     */
    private final String hostname;

    /**
     * The name of the bucket
     */
    private final String bucket;

    /**
     * The username (used for http auth).
     */
    private final String username;

    /**
     * THe password of the bucket (used for http auth).
     */
    private final String password;

    /**
     * The config stream where the configs are emitted into.
     */
    private final IConfigurable configurable;
    private final SSLEngineFactory sslEngineFactory;

    /**
     * Creates a new config pipeline.
     *
     * @param environment
     *            the stateful environment.
     * @param hostname
     *            hostname of the remote server.
     * @param configStream
     *            config stream where to send the configs.
     */
    public ConfigPipeline(final ClientEnvironment environment, final String hostname,
            final IConfigurable configurable) {
        this.hostname = hostname;
        this.bucket = environment.bucket();
        this.username = environment.username();
        this.password = environment.password();
        this.configurable = configurable;
        this.environment = environment;
        if (environment.sslEnabled()) {
            this.sslEngineFactory = new SSLEngineFactory(environment);
        } else {
            this.sslEngineFactory = null;
        }
    }

    /**
     * Init the pipeline with the HTTP codec and our handler which does all the json chunk
     * decoding and parsing.
     *
     * If trace logging is enabled also add the logging handler so we can figure out whats
     * going on when debugging.
     */
    @Override
    protected void initChannel(final Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        if (environment.sslEnabled()) {
            pipeline.addLast(new SslHandler(sslEngineFactory.get()));
        }
        if (LOGGER.isTraceEnabled()) {
            pipeline.addLast(new LoggingHandler(LogLevel.TRACE));
        }

        pipeline.addLast(new HttpClientCodec()).addLast(new StartStreamHandler(bucket, username, password))
                .addLast(new ConfigHandler(hostname, configurable, environment));
    }

}

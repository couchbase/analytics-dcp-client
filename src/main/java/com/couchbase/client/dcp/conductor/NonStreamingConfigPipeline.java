/*
 * Copyright (c) 2018 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import static org.apache.hyracks.util.NetworkUtil.toHostPort;

import java.net.InetSocketAddress;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.SSLEngineFactory;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpClientCodec;
import com.couchbase.client.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.deps.io.netty.handler.logging.LoggingHandler;
import com.couchbase.client.deps.io.netty.handler.ssl.SslHandler;

public class NonStreamingConfigPipeline extends ChannelInitializer<Channel> {

    /**
     * The logger used.
     */
    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * The stateful environment.
     */
    private final ClientEnvironment environment;

    /**
     * Hostname used to replace $HOST parts in the config when used against localhost.
     */
    private final InetSocketAddress address;

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
    private final SSLEngineFactory sslEngineFactory;

    private final MutableObject<Throwable> failure;
    private final MutableObject<CouchbaseBucketConfig> config;

    /**
     * Creates a new config pipeline.
     *
     * @param environment
     *            the stateful environment.
     * @param hostname
     *            hostname of the remote server.
     * @param port
     * @param configStream
     *            config stream where to send the configs.
     */
    public NonStreamingConfigPipeline(final ClientEnvironment environment, final InetSocketAddress address,
            MutableObject<Throwable> failure, MutableObject<CouchbaseBucketConfig> config) throws Exception {
        this.address = address;
        this.bucket = environment.bucket();
        Pair<String, String> creds = environment.credentialsProvider().get(address);
        this.username = creds.getLeft();
        this.password = creds.getRight();
        this.failure = failure;
        this.config = config;
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

        pipeline.addLast(new HttpClientCodec())
                .addLast(new RequestConfigHandler(bucket, username, password, config, failure))
                .addLast(new NonStreamingConfigHandler(address, environment, config, failure));
    }

}

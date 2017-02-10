/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.ConfigPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.deps.io.netty.channel.ChannelOption;

/**
 * The {@link HttpStreamingConfigProvider}s only purpose is to keep new configs coming in all the time in a resilient manner.
 *
 * @author Michael Nitschinger
 */
public class HttpStreamingConfigProvider implements ConfigProvider, IConfigurable {

    private static final CouchbaseLogger LOGGER =
            CouchbaseLoggerFactory.getInstance(HttpStreamingConfigProvider.class);
    private final AtomicReference<List<String>> hostnames;
    private final ClientEnvironment env;
    private CouchbaseBucketConfig config;
    private volatile boolean refreshed = false;
    private volatile boolean failure = false;
    private volatile Throwable cause;
    private final ChannelFutureListener closeListener = (ChannelFuture future) -> {
        synchronized (HttpStreamingConfigProvider.this) {
            LOGGER.log(CouchbaseLogLevel.WARN, "Channel has been closed");
            if (!refreshed) {
                LOGGER.log(CouchbaseLogLevel.WARN, "Before it is refereshed. Need to wake up the waiting thread");
                failure = true;
                HttpStreamingConfigProvider.this.notifyAll();
            }
        }
    };

    public HttpStreamingConfigProvider(ClientEnvironment env) {
        this.env = env;
        this.hostnames = new AtomicReference<>(env.hostnames());
    }

    @Override
    public void refresh() throws Throwable {
        refreshed = false;
        tryConnectHosts();
    }

    @Override
    public CouchbaseBucketConfig config() {
        return config;
    }

    private void tryConnectHosts() throws Throwable {
        Throwable cause = null;
        List<String> hosts = this.hostnames.get();
        for (int i = 0; i < hosts.size(); i++) {
            if (tryConnectHost(hosts.get(i))) {
                return;
            } else {
                if (cause == null) {
                    cause = this.cause;
                } else {
                    cause.addSuppressed(this.cause);
                }
            }
        }
        throw cause;
    }

    private boolean tryConnectHost(final String hostname) throws InterruptedException {
        int attempt = 0;
        ByteBufAllocator allocator =
                env.poolBuffers() ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
        while (attempt < env.configProviderReconnectMaxAttempts()) {
            Bootstrap bootstrap = new Bootstrap()
                    .remoteAddress(hostname,
                            env.sslEnabled() ? env.bootstrapHttpSslPort() : env.bootstrapHttpDirectPort())
                    .option(ChannelOption.ALLOCATOR, allocator)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) env.socketConnectTimeout())
                    .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
                    .handler(new ConfigPipeline(env, hostname, this)).group(env.eventLoopGroup());
            ChannelFuture connectFuture = bootstrap.connect();
            try {
                connectFuture.await();
                if (connectFuture.isSuccess()) {
                    waitForConfig(connectFuture);
                    if (refreshed) {
                        failure = false;
                        cause = null;
                        return true;
                    }
                } else {
                    cause = connectFuture.cause();
                }
            } finally {
                LOGGER.log(CouchbaseLogLevel.WARN, "Closing the channel");
                connectFuture.channel().close().await();
                LOGGER.log(CouchbaseLogLevel.WARN, "Channel closed");
            }
            if (cause != null && !(cause instanceof SocketException)) {
                return false;
            }
            synchronized (this) {
                wait(200);
            }
            attempt++;
        }
        return false;
    }

    private synchronized void waitForConfig(ChannelFuture connectFuture) throws InterruptedException {
        failure = false;
        connectFuture.channel().closeFuture().addListener(closeListener);
        while (!failure && !refreshed) {
            this.wait();
        }
    }

    @Override
    public synchronized void configure(CouchbaseBucketConfig config) {
        this.config = config;
        List<String> newNodes = new ArrayList<>();
        for (NodeInfo node : config.nodes()) {
            newNodes.add(node.hostname().getHostAddress());
        }
        LOGGER.warn("Updated config stream node list to {}.", newNodes);
        hostnames.set(newNodes);
        refreshed = true;
        notifyAll();
    }

}

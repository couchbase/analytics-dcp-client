/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

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

public class HttpStreamingConfigProvider implements ConfigProvider, IConfigurable {

    private static final CouchbaseLogger LOGGER =
            CouchbaseLoggerFactory.getInstance(HttpStreamingConfigProvider.class);
    private final List<String> hostnames;
    private final ClientEnvironment env;
    private volatile CouchbaseBucketConfig config;
    private volatile boolean refreshed = false;
    private volatile boolean failure = false;
    private volatile Throwable cause;
    private final ChannelFutureListener closeListener = (ChannelFuture future) -> {
        synchronized (HttpStreamingConfigProvider.this) {
            LOGGER.log(CouchbaseLogLevel.DEBUG, "Channel has been closed");
            if (!refreshed) {
                LOGGER.log(CouchbaseLogLevel.DEBUG, "Before it is refereshed. Need to wake up the waiting thread");
                refreshed = true;
                failure = true;
                HttpStreamingConfigProvider.this.notifyAll();
            }
        }
    };

    public HttpStreamingConfigProvider(ClientEnvironment env) {
        this.env = env;
        this.hostnames = new ArrayList<>(env.hostnames());
    }

    @Override
    public void refresh() throws Throwable {
        tryConnectHosts();
    }

    @Override
    public CouchbaseBucketConfig config() {
        return config;
    }

    private void tryConnectHosts() throws Throwable {
        Throwable cause = null;
        for (int i = 0; i < hostnames.size(); i++) {
            if (tryConnectHost(hostnames.get(i))) {
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
            failure = false;
            refreshed = false;
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
                } else {
                    fail(connectFuture.cause());
                }
            } finally {
                LOGGER.log(CouchbaseLogLevel.DEBUG, "Closing the channel");
                connectFuture.channel().close().await();
                LOGGER.log(CouchbaseLogLevel.DEBUG, "Channel closed");
            }
            if (!failure) {
                return true;
            }
            if (cause != null && !(cause instanceof SocketException)) {
                return false;
            }
            synchronized (this) {
                wait(100);
            }
            attempt++;
        }
        return false;
    }

    private synchronized void waitForConfig(ChannelFuture connectFuture) throws InterruptedException {
        connectFuture.channel().closeFuture().addListener(closeListener);
        while (!refreshed) {
            this.wait();
        }
    }

    @Override
    public synchronized void configure(CouchbaseBucketConfig config) {
        this.config = config;
        this.cause = null;
        hostnames.clear();
        for (NodeInfo node : config.nodes()) {
            hostnames.add(node.hostname().getHostAddress());
        }
        LOGGER.debug("Updated config stream node list to {}.", hostnames);
        refreshed = true;
        notifyAll();
    }

    @Override
    public synchronized void fail(Throwable throwable) {
        failure = true;
        cause = throwable;
        refreshed = true;
        notifyAll();
    }

}

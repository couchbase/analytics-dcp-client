/*
 * Copyright (c) 2018 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.core.utils.NetworkAddress;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.error.BadBucketConfigException;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelOption;

public class NonStreamingConfigProvider implements ConfigProvider, IConfigurable {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<NetworkAddress, Set<Integer>> sockets;
    private final ClientEnvironment env;
    private volatile CouchbaseBucketConfig config;
    private volatile Throwable cause;

    public NonStreamingConfigProvider(ClientEnvironment env) {
        this.env = env;
        this.sockets = new HashMap<>();
        int defaultPort = env.sslEnabled() ? env.bootstrapHttpSslPort() : env.bootstrapHttpDirectPort();
        for (String hostname : env.hostnames()) {
            // TODO: Make it work for IPv6
            int port = hostname.contains(":") ? Integer.parseInt(hostname.substring(hostname.indexOf(':') + 1))
                    : defaultPort;
            String host = hostname.indexOf(':') > -1 ? hostname.substring(0, hostname.indexOf(':')) : hostname;
            NetworkAddress address = NetworkAddress.create(host);
            LOGGER.error("Adding a config node " + hostname + ":" + port);
            Set<Integer> ports = sockets.computeIfAbsent(address, arg -> new HashSet<>());
            ports.add(port);
        }
    }

    @Override
    public void refresh() throws Throwable {
        refresh(env.configProviderAttemptTimeout(), env.configProviderTotalTimeout(),
                env.configProviderReconnectDelay());
    }

    @Override
    public void refresh(long attemptTimeout, long totalTimeout) throws Throwable {
        refresh(attemptTimeout, totalTimeout, env.configProviderReconnectDelay());
    }

    @Override
    public void refresh(long attemptTimeout, long totalTimeout, Delay delay) throws Throwable {
        tryConnectHosts(attemptTimeout, totalTimeout, delay);
    }

    @Override
    public CouchbaseBucketConfig config() {
        return config;
    }

    private void tryConnectHosts(long attemptTimeout, long totalTimeout, Delay delay) throws Throwable {
        for (Entry<NetworkAddress, Set<Integer>> addresses : sockets.entrySet()) {
            for (Integer port : addresses.getValue()) {
                if (tryConnectHost(addresses.getKey(), port, attemptTimeout, totalTimeout, delay)) {
                    return;
                }
            }
        }
        throw this.cause;
    }

    private boolean tryConnectHost(NetworkAddress hostname, int port, long attemptTimeout, long totalTimeout,
            Delay delay) throws Exception {
        int attempt = 0;
        ByteBufAllocator allocator =
                env.poolBuffers() ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
        final long startTime = System.nanoTime();
        while (true) {
            attempt++;
            MutableObject<Throwable> failure = new MutableObject<>();
            MutableObject<CouchbaseBucketConfig> config = new MutableObject<>();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.log(Level.INFO, "Getting bucket config from {}:{}", hostname.nameOrAddress(), port);
            }
            Bootstrap bootstrap =
                    new Bootstrap().remoteAddress(hostname.address(), port).option(ChannelOption.ALLOCATOR, allocator)
                            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) attemptTimeout)
                            .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
                            .handler(new NonStreamingConfigPipeline(env, hostname.address(), port, failure, config))
                            .group(env.eventLoopGroup());
            ChannelFuture connectFuture = bootstrap.connect();
            try {
                connectFuture.await(attemptTimeout + 100);
                connectFuture.cancel(true);
                if (connectFuture.isSuccess()) {
                    waitForConfig(config, failure, attemptTimeout);
                } else {
                    fail(connectFuture.cause());
                    failure.setValue(connectFuture.cause());
                }
            } finally {
                LOGGER.log(Level.DEBUG, "Closing the channel");
                connectFuture.channel().close().await();
                LOGGER.log(Level.DEBUG, "Channel closed");
            }
            if (failure.getValue() == null) {
                configure(config.getValue());
                return true;
            } else {
                fail(failure.getValue());
            }
            if (cause != null && !(cause instanceof IOException)) {
                return false;
            }
            attempt++;
            if (elapsed(startTime, totalTimeout)) {
                return false;
            }
            Thread.sleep(delay.calculate(attempt));
        }
    }

    private boolean elapsed(long startTimeNano, long timeoutMillis) {
        return System.nanoTime() - startTimeNano >= TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }

    private void waitForConfig(MutableObject<CouchbaseBucketConfig> config, MutableObject<Throwable> failure,
            long attemptTimeout) throws InterruptedException {
        final long startTime = System.nanoTime();
        synchronized (config) {
            while (config.getValue() == null && failure.getValue() == null && !elapsed(startTime, attemptTimeout)) {
                config.wait(attemptTimeout);
            }
            if (config.getValue() == null && failure.getValue() == null) {
                failure.setValue(new TimeoutException(attemptTimeout + "ms passed before obtaining configurations"));
            }
        }
    }

    @Override
    public synchronized void configure(CouchbaseBucketConfig config) throws Exception {
        if (config.numberOfPartitions() == 0) {
            throw new BadBucketConfigException("Bucket configuration doesn't contain a vbucket map");
        }
        this.config = config;
        this.cause = null;
        for (NodeInfo node : config.nodes()) {
            Integer port = (env.sslEnabled() ? node.sslServices() : node.services()).get(ServiceType.CONFIG);
            LOGGER.error("Adding a config node " + node.hostname() + ":" + port);
            NetworkAddress address = node.hostname();
            Set<Integer> ports = sockets.computeIfAbsent(address, args -> new HashSet<>());
            ports.add(port);
        }
        LOGGER.debug("Updated config stream node list to {}.", sockets);
        notifyAll();
    }

    @Override
    public synchronized void fail(Throwable throwable) {
        LOGGER.log(Level.WARN, "Failed getting bucket config", throwable);
        cause = throwable;
        notifyAll();
    }
}

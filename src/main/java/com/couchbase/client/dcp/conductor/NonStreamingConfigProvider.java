/*
 * Copyright 2018-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.conductor;

import static com.couchbase.client.core.env.NetworkResolution.EXTERNAL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.config.AlternateAddress;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.time.Delay;
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
    private static final long MIN_MILLIS_PER_REFRESH = 1000;
    private final Set<InetSocketAddress> sockets = new LinkedHashSet<>();

    private final ClientEnvironment env;
    private volatile CouchbaseBucketConfig config;
    private Span refreshPeriod;
    private volatile String uuid;
    private volatile Throwable cause;

    NonStreamingConfigProvider(ClientEnvironment env) {
        this.env = env;
        this.uuid = env.uuid();
        sockets.addAll(env.clusterAt());
        refreshPeriod = Span.start(0, TimeUnit.NANOSECONDS);
        LOGGER.info("Adding config nodes: " + sockets);
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
        if (refreshPeriod.elapsed()) {
            tryConnectHosts(attemptTimeout, totalTimeout, delay);
            refreshPeriod = Span.start(MIN_MILLIS_PER_REFRESH, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public CouchbaseBucketConfig config() {
        return config;
    }

    private void tryConnectHosts(long attemptTimeout, long totalTimeout, Delay delay) throws Throwable {
        for (InetSocketAddress socket : sockets) {
            if (tryConnectHost(socket, attemptTimeout, totalTimeout, delay)) {
                return;
            }
        }
        throw cause;
    }

    private boolean tryConnectHost(InetSocketAddress address, long attemptTimeout, long totalTimeout, Delay delay)
            throws Exception {
        int attempt = 0;
        ByteBufAllocator allocator =
                env.poolBuffers() ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
        final long startTime = System.nanoTime();
        while (true) {
            attempt++;
            MutableObject<Throwable> failure = new MutableObject<>();
            MutableObject<CouchbaseBucketConfig> config = new MutableObject<>();
            LOGGER.info("Getting bucket config from {}", address);
            Bootstrap bootstrap = new Bootstrap().remoteAddress(address).option(ChannelOption.ALLOCATOR, allocator)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) attemptTimeout)
                    .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
                    .handler(new NonStreamingConfigPipeline(env, address, failure, config, uuid))
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
        if (uuid.isEmpty()) {
            uuid = Conductor.getUuid(config.uri());
        }

        if (env.dynamicConfigurationNodes()) {
            List<InetSocketAddress> configNodes = new ArrayList<>(config.nodes().size());
            for (NodeInfo node : config.nodes()) {
                int port = (env.sslEnabled() ? node.sslServices() : node.services()).get(ServiceType.CONFIG);
                InetSocketAddress address = new InetSocketAddress(node.hostname(), port);
                if (env.networkResolution().equals(EXTERNAL)) {
                    AlternateAddress aa = node.alternateAddresses().get(EXTERNAL.name());
                    if (aa == null) {
                        LOGGER.info("omitting node {} which does not provide an external alternate address", address);
                        continue;
                    }
                    Map<ServiceType, Integer> services = env.sslEnabled() ? aa.sslServices() : aa.services();
                    if (services.containsKey(ServiceType.CONFIG)) {
                        int altPort = services.get(ServiceType.CONFIG);
                        InetSocketAddress altAddress = new InetSocketAddress(aa.hostname(), altPort);
                        LOGGER.info("Adding a config node {} at alternate address {}", address, altAddress);
                        configNodes.add(altAddress);
                    } else {
                        LOGGER.info(
                                "omitting node {} which does not provide the config service on its external alternate address",
                                address);
                    }
                } else {
                    LOGGER.info("Adding a config node {}", address);
                    configNodes.add(address);
                }
            }
            if (!configNodes.isEmpty()) {
                sockets.clear();
                sockets.addAll(configNodes);
            } else {
                LOGGER.warn("New Configuration doesn't contain any config nodes {}.", config);
                throw new BadBucketConfigException("New Configuration doesn't contain any config nodes");
            }
            LOGGER.debug("Updated config stream node list to {}.", sockets);
        }
        this.config = config;
        this.cause = null;
        notifyAll();
    }

    @Override
    public synchronized void fail(Throwable throwable) {
        LOGGER.log(Level.WARN, "Failed getting bucket config", throwable);
        cause = throwable;
        notifyAll();
    }
}

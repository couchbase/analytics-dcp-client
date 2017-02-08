/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.conductor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.ConfigPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;

import rx.Completable;

/**
 * The {@link HttpStreamingConfigProvider}s only purpose is to keep new configs coming in all the time in a resilient manner.
 *
 * @author Michael Nitschinger
 */
public class HttpStreamingConfigProvider implements ConfigProvider, IConfigurable {

    private static final CouchbaseLogger LOGGER =
            CouchbaseLoggerFactory.getInstance(HttpStreamingConfigProvider.class);
    private final AtomicReference<List<String>> hostnames;
    private volatile CouchbaseBucketConfig config;
    private final ClientEnvironment env;

    public HttpStreamingConfigProvider(ClientEnvironment env) {
        this.env = env;
        this.hostnames = new AtomicReference<>(env.hostnames());
    }

    @Override
    public Completable refresh() {
        return tryConnectHosts();
    }

    @Override
    public CouchbaseBucketConfig config() {
        return config;
    }

    private Completable tryConnectHosts() {
        List<String> hosts = this.hostnames.get();
        Completable chain = tryConnectHost(hosts.get(0));
        for (int i = 1; i < hosts.size(); i++) {
            final String h = hosts.get(i);
            chain = chain.onErrorResumeNext(throwable -> {
                LOGGER.warn("Could not get config from Node, trying next in list.", throwable);
                return tryConnectHost(h);
            });
        }
        return chain;
    }

    private Completable tryConnectHost(final String hostname) {
        ByteBufAllocator allocator =
                env.poolBuffers() ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
        final Bootstrap bootstrap = new Bootstrap()
                .remoteAddress(hostname, env.sslEnabled() ? env.bootstrapHttpSslPort() : env.bootstrapHttpDirectPort())
                .option(ChannelOption.ALLOCATOR, allocator)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) env.socketConnectTimeout())
                .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
                .handler(new ConfigPipeline(env, hostname, this)).group(env.eventLoopGroup());
        return Completable
                .create(subscriber -> bootstrap.connect().addListener(new GenericFutureListener<ChannelFuture>() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            Channel channel = future.channel();
                            channel.closeFuture().addListener(new GenericFutureListener<ChannelFuture>() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    subscriber.onCompleted();
                                }
                            });
                            LOGGER.debug("Successfully established config connection to Socket {}",
                                    channel.remoteAddress());
                        } else {
                            subscriber.onError(future.cause());
                        }
                    }
                }));
    }

    @Override
    public void configure(CouchbaseBucketConfig config) {
        this.config = config;
        List<String> newNodes = new ArrayList<>();
        for (NodeInfo node : config.nodes()) {
            newNodes.add(node.hostname().getHostAddress());
        }
        LOGGER.trace("Updated config stream node list to {}.", newNodes);
        hostnames.set(newNodes);
    }

}

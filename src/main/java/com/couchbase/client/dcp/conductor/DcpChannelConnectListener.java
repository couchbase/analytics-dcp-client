/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelFutureListener;

public class DcpChannelConnectListener implements ChannelFutureListener {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpChannelConnectListener.class);

    private final DcpChannel channel;
    private volatile boolean complete = false;

    public DcpChannelConnectListener(DcpChannel channel) {
        this.channel = channel;
    }

    @Override
    public synchronized void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
            channel.setChannel(future.channel());
            LOGGER.warn("Connected to Node {}", channel.getInetAddress());
        }
        complete = true;
        notifyAll();
    }

    public synchronized void listen(ChannelFuture connectFuture) throws Throwable {
        complete = false;
        connectFuture.addListener(this);
        while (!complete) {
            wait();
        }
        if (!connectFuture.isSuccess()) {
            throw connectFuture.cause();
        }
        LOGGER.warn("Connection established");
    }
}

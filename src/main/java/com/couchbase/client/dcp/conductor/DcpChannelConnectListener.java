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
    private volatile ChannelFuture future;
    private volatile boolean complete = false;

    public DcpChannelConnectListener(DcpChannel channel) {
        this.channel = channel;
    }

    @Override
    public synchronized void operationComplete(ChannelFuture future) throws Exception {
        this.future = future;
        if (future.isSuccess()) {
            channel.setChannel(future.channel());
            LOGGER.info("Connected to Node {}", channel.getInetAddress());
            // attach callback which listens on future close and dispatches a
            // reconnect if needed.
            future.channel().closeFuture().addListener(channel.getCloseListener());
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
        if (!this.future.isSuccess()) {
            throw this.future.cause();
        }
        LOGGER.debug("Connection established");
    }

}

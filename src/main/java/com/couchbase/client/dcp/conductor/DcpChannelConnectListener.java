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
            if (channel.getState() == State.DISCONNECTING) {
                LOGGER.info("Connected Node {}, but got instructed to disconnect in " + "the meantime.",
                        channel.getInetAddress());
                // isShutdown before we could finish the connect :/
                channel.disconnect();
            } else {
                LOGGER.info("Connected to Node {}", channel.getInetAddress());
                // attach callback which listens on future close and dispatches a
                // reconnect if needed.
                channel.setState(State.CONNECTED);
                future.channel().closeFuture().addListener(channel.getCloseListener());
            }
        } else {
            if (channel.getState() == State.DISCONNECTING) {
                // Don't retry
                LOGGER.info("Connect attempt to {} failed.", channel.getInetAddress(), future.cause());
                channel.setState(State.DISCONNECTED);
            }
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

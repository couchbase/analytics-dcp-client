/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.events.ChannelDroppedEvent;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelFutureListener;

public class DcpChannelCloseListener implements ChannelFutureListener {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpChannelCloseListener.class);

    private final DcpChannel channel;

    public DcpChannelCloseListener(DcpChannel channel) {
        this.channel = channel;
    }

    @Override
    public synchronized void operationComplete(ChannelFuture future) throws Exception {
        LOGGER.warn("DCP Connection dropped");
        channel.clear();
        // channel was closed. If the state is disconnecting, then this should be fine
        // otherwise, this should attempt to restart the connection
        switch (channel.getState()) {
            case CONNECTED:
                revive();
                break;
            case DISCONNECTING:
                channel.setState(State.DISCONNECTED);
                break;
            default:
                LOGGER.error("This should never happen");
                channel.getEnv().eventBus().publish(new ImpossibleEvent());
                break;
        }
    }

    private void revive() {
        synchronized (channel) {
            channel.setState(State.DISCONNECTED);
            try {
                LOGGER.warn("trying to reconnect");
                wait(200);
                channel.connect();
            } catch (Throwable th) {
                LOGGER.warn("Failed to re-establish a failed test. Notifying the user");
                channel.getEnv().eventBus().publish(new ChannelDroppedEvent(channel, th));
            }
        }
    }

}

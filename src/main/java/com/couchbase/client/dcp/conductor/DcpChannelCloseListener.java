/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.events.ChannelDroppedEvent;
import com.couchbase.client.dcp.events.ImpossibleEvent;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelFutureListener;

public class DcpChannelCloseListener implements ChannelFutureListener {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpChannelCloseListener.class);

    private final DcpChannel channel;

    public DcpChannelCloseListener(DcpChannel channel) {
        this.channel = channel;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
        LOGGER.debug("DCP Connection dropped");
        synchronized (channel) {
            // channel was closed. If the state is disconnecting, then this should be fine
            // otherwise, this should attempt to restart the connection
            switch (channel.getState()) {
                case CONNECTED:
                    channel.setChannelDroppedReported(true);
                    channel.getEnv().eventBus().publish(new ChannelDroppedEvent(channel, null));
                    break;
                case DISCONNECTING:
                    channel.setState(State.DISCONNECTED);
                    break;
                default:
                    LOGGER.error("Unexpected state {} on dropped connection for channel {}", channel.getState(),
                            channel);
                    channel.getEnv().eventBus().publish(new ImpossibleEvent());
                    break;
            }
        }
    }
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.conductor.DcpChannel;

public class ChannelDroppedEvent implements DcpEvent {
    private final DcpChannel channel;
    private final Throwable cause;

    public ChannelDroppedEvent(DcpChannel channel, Throwable cause) {
        this.channel = channel;
        this.cause = cause;
    }

    @Override
    public Type getType() {
        return Type.CHANNEL_DROPPED;
    }

    public DcpChannel getChannel() {
        return channel;
    }

    public Throwable getCause() {
        return cause;
    }

}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import java.util.Map;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.dcp.conductor.DcpChannel;

public class ChannelDroppedEvent implements CouchbaseEvent {
    private final DcpChannel channel;
    private final Throwable cause;

    public ChannelDroppedEvent(DcpChannel channel, Throwable cause) {
        this.channel = channel;
        this.cause = cause;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    @Override
    public Map<String, Object> toMap() {
        return null;
    }

    public DcpChannel getChannel() {
        return channel;
    }

    public Throwable getCause() {
        return cause;
    }

}

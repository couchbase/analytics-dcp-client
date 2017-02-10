/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import java.util.Map;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.utils.Events;
import com.couchbase.client.dcp.message.StreamEndReason;

/**
 * Event published when stream has stopped activity.
 */
public class StreamEndEvent implements CouchbaseEvent {
    private final short partition;
    private final StreamEndReason reason;

    public StreamEndEvent(short partition, StreamEndReason reason) {
        this.partition = partition;
        this.reason = reason;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    public short partition() {
        return partition;
    }

    public StreamEndReason reason() {
        return reason;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = Events.identityMap(this);
        result.put("partition", partition);
        result.put("reason", reason);
        return result;
    }

    @Override
    public String toString() {
        return "StreamEndEvent{" + "partition=" + partition + "reason=" + reason + '}';
    }
}

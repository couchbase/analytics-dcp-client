/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import java.util.Map;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.utils.Events;

/**
 * Event published when the connector has failed to move partition to new node during failover/rebalance.
 */
public class FailedToMovePartitionEvent implements CouchbaseEvent {

    private final short partition;
    private final Throwable error;

    public FailedToMovePartitionEvent(short partition, Throwable error) {
        this.partition = partition;
        this.error = error;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = Events.identityMap(this);
        result.put("partition", partition);
        result.put("error", error);
        return result;
    }

    /**
     * The partition ID which has caused issue
     */
    public short partition() {
        return partition;
    }

    /**
     * Error object, describing the issue
     */
    public Throwable error() {
        return error;
    }

    @Override
    public String toString() {
        return "FailedToMovePartitionEvent{" + "partition=" + partition + "error=" + error + '}';
    }
}

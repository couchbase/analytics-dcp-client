/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import java.net.InetAddress;
import java.util.Map;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.utils.Events;

/**
 * Event published when the connector has failed to add new node during failover/rebalance.
 */
public class FailedToAddNodeEvent implements CouchbaseEvent {
    private final InetAddress node;
    private final Throwable error;

    public FailedToAddNodeEvent(InetAddress node, Throwable error) {
        this.node = node;
        this.error = error;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    /**
     * The address of the node
     */
    public InetAddress node() {
        return node;
    }

    /**
     * Error object, describing the issue
     */
    public Throwable error() {
        return error;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = Events.identityMap(this);
        result.put("node", node);
        result.put("error", error);
        return result;
    }

    @Override
    public String toString() {
        return "FailedToAddNodeEvent{" + "node=" + node + "error=" + error + '}';
    }
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.util.Map;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;

public class ImpossibleEvent implements CouchbaseEvent {

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    @Override
    public Map<String, Object> toMap() {
        return null;
    }

}

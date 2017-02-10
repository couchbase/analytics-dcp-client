/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.util.Map;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;

public class NotMyVBuvketEvent implements CouchbaseEvent {

    private final DcpChannel channel;
    private final short vbid;

    public NotMyVBuvketEvent(DcpChannel channel, short vbid) {
        this.channel = channel;
        this.vbid = vbid;
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

    public short getVbid() {
        return vbid;
    }
}

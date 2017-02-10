/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import java.util.Map;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;

public class RollbackEvent implements CouchbaseEvent {

    private final short vbid;
    private final long seq;

    public RollbackEvent(short vbid, long seq) {
        this.vbid = vbid;
        this.seq = seq;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    @Override
    public Map<String, Object> toMap() {
        return null;
    }

    public short getVbid() {
        return vbid;
    }

    public long getSeq() {
        return seq;
    }

}

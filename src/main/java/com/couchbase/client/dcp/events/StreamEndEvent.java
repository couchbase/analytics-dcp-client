/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.state.PartitionState;

/**
 * Event published when stream has stopped activity.
 */
public class StreamEndEvent implements DcpEvent {
    private final PartitionState state;
    private StreamEndReason reason;

    public StreamEndEvent(PartitionState state) {
        this.state = state;
        reason = StreamEndReason.INVALID;
    }

    @Override
    public Type getType() {
        return Type.STREAM_END;
    }

    public short partition() {
        return state.vbid();
    }

    public StreamEndReason reason() {
        return reason;
    }

    public void reset() {
        reason = StreamEndReason.INVALID;
    }

    public void setReason(StreamEndReason reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "StreamEndEvent{" + "partition=" + state.vbid() + "reason=" + reason + '}';
    }

    public PartitionState getState() {
        return state;
    }
}

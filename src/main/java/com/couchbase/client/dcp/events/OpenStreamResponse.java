package com.couchbase.client.dcp.events;

import org.apache.hyracks.util.Span;

import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.util.MemcachedStatus;

public class OpenStreamResponse implements PartitionDcpEvent {
    private final StreamPartitionState state;
    private final short status;

    public OpenStreamResponse(StreamPartitionState state, short status) {
        this.state = state;
        this.status = status;
        state.calculateNextDelay(status);
    }

    @Override
    public Type getType() {
        return Type.OPEN_STREAM_RESPONSE;
    }

    public short getStatus() {
        return status;
    }

    @Override
    public StreamPartitionState getPartitionState() {
        return state;
    }

    @Override
    public String toString() {
        return "{\"open-stream-response\":\"" + MemcachedStatus.toString(status) + "\"}";
    }

    @Override
    public Span delay() {
        return state.getDelay();
    }
}

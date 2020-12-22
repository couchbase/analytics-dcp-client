package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.util.MemcachedStatus;

public class OpenStreamRollbackResponse extends OpenStreamResponse {
    private final long rollbackSeq;

    public OpenStreamRollbackResponse(StreamPartitionState state, long rollbackSeq) {
        super(state, MemcachedStatus.ROLLBACK);
        this.rollbackSeq = rollbackSeq;
    }

    @Override
    public Type getType() {
        return Type.OPEN_STREAM_ROLLBACK_RESPONSE;
    }

    public long getRollbackSeq() {
        return rollbackSeq;
    }

    @Override
    public String toString() {
        return "{\"open-stream-response\":\"" + MemcachedStatus.toString(getStatus()) + "\", " + "\"rollback-seq\":"
                + Long.toUnsignedString(rollbackSeq) + "}";
    }
}

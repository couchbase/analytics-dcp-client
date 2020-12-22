package com.couchbase.client.dcp.events;

import org.apache.hyracks.util.Span;

public interface DcpEvent {
    enum Type {
        STREAM_END,
        CHANNEL_DROPPED,
        OPEN_STREAM_RESPONSE,
        OPEN_STREAM_ROLLBACK_RESPONSE,
        FAILOVER_LOG_RESPONSE,
        UNEXPECTED_FAILURE,
        DISCONNECT
    }

    Type getType();

    default Span delay() {
        return Span.ELAPSED;
    }
}

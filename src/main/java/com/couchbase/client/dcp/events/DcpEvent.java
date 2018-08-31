package com.couchbase.client.dcp.events;

import java.util.concurrent.TimeUnit;

import org.apache.hyracks.util.Span;

public interface DcpEvent {
    public static final Span ELAPSED = Span.start(0, TimeUnit.NANOSECONDS);

    enum Type {
        STREAM_END,
        CHANNEL_DROPPED,
        OPEN_STREAM_RESPONSE,
        FAILOVER_LOG_RESPONSE,
        UNEXPECTED_FAILURE,
        DISCONNECT
    }

    Type getType();

    default Span delay() {
        return ELAPSED;
    }
}

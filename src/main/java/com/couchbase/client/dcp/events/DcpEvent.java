package com.couchbase.client.dcp.events;

public interface DcpEvent {
    enum Type {
        STREAM_END,
        CHANNEL_DROPPED,
        OPEN_STREAM_RESPONSE,
        FAILOVER_LOG_RESPONSE,
        UNEXPECTED_FAILURE,
        DISCONNECT
    }

    Type getType();
}

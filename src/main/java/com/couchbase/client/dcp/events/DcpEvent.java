package com.couchbase.client.dcp.events;

public interface DcpEvent {
    enum Type {
        STREAM_END,
        PARTITION_UUID_CHANGED,
        CHANNEL_DROPPED,
        OPEN_STREAM_RESPONSE,
        FAILOVER_LOG_RESPONSE,
        UNEXPECTED_FAILURE,
        DISCONNECT
    }

    Type getType();
}

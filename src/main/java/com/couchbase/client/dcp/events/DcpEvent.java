package com.couchbase.client.dcp.events;

public interface DcpEvent {
    enum Type {
        STREAM_END,
        CHANNEL_DROPPED,
        ROLLBACK,
        NOT_MY_VBUCKET,
        FAILOVER_UPDATE,
        UNEXPECTED_FAILURE,
        DISCONNECT
    }

    Type getType();
}

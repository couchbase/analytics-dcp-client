package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.SystemEventHandler;

public interface EventBus {
    void publish(DcpEvent event);

    void subscribe(SystemEventHandler handler);
}

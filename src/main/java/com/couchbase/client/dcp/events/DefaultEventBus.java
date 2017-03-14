package com.couchbase.client.dcp.events;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.dcp.SystemEventHandler;

public class DefaultEventBus implements EventBus {
    List<SystemEventHandler> subscribers = new ArrayList<>();

    @Override
    public synchronized void publish(DcpEvent event) {
        for (SystemEventHandler subscriber : subscribers) {
            subscriber.onEvent(event);
        }
    }

    @Override
    public synchronized void subscribe(SystemEventHandler handler) {
        subscribers.add(handler);
    }
}

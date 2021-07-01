/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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

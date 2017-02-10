/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

import com.couchbase.client.core.event.CouchbaseEvent;

/**
 * This interface acts as a callback on the {@link Client#systemEventHandler(SystemEventHandler)} API
 * that allows one to react to system events.
 */
@FunctionalInterface
public interface SystemEventHandler {

    /**
     * Called every time when a system event happens that should be handled by
     * consumers of the {@link Client}.
     *
     * @param event
     *            the system event happening.
     */
    void onEvent(CouchbaseEvent event);
}

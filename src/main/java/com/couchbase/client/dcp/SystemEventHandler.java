/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp;

import com.couchbase.client.dcp.events.DcpEvent;

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
    void onEvent(DcpEvent event);
}

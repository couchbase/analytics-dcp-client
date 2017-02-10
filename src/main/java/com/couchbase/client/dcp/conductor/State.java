/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

public enum State {
    /**
     * The component is currently disconnected.
     */
    DISCONNECTED,

    /**
     * The component is currently connecting or reconnecting.
     */
    CONNECTING,

    /**
     * The component is connected without degradation.
     */
    CONNECTED,

    /**
     * The component is disconnecting.
     */
    DISCONNECTING
}

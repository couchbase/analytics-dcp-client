/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

/**
 * Up to which point the DCP stream should run.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public enum StreamTo {
    /**
     * Stop "now", where now is the time when the state is initialized that way.
     */
    NOW,

    /**
     * Never stop streaming automatically, but manual stream stop is of course possible.
     */
    INFINITY
}

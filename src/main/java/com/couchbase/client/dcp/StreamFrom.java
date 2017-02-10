/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

/**
 * From which point in time to start the DCP stream.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public enum StreamFrom {
    /**
     * Start at the very beginning - will stream all docs in the bucket.
     */
    BEGINNING,

    /**
     * Start "now", where now is a time point of execution in the running program where the state
     * is gathered from each partition. Mutations will be streamed after this point.
     */
    NOW
}

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

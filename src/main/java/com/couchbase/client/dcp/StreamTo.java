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

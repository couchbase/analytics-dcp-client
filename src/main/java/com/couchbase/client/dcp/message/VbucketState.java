/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.message;

public enum VbucketState {
    /**
     * Any live state (except DEAD).
     */
    ANY(0),
    /**
     * Actively servicing a partition.
     */
    ACTIVE(1),
    /**
     * Servicing a partition as a replica only.
     */
    REPLICA(2),
    /**
     * Pending active.
     */
    PENDING(3),
    /**
     * Not in use, pending deletion.
     */
    DEAD(4);

    private final int value;

    VbucketState(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}

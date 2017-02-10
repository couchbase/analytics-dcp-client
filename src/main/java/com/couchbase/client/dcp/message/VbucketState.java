/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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

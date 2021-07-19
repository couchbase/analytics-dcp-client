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

/**
 * Flags, used in snapshot messages.
 */
public enum SnapshotMarkerFlags {
    /**
     * Specifies that the snapshot contains in-memory items only.
     */
    MEMORY(0x01),

    /**
     * Specifies that the snapshot contains on-disk items only.
     */
    DISK(0x02),

    /**
     * An internally used flag for intra-cluster replication to help to keep in-memory datastructures look similar.
     */
    CHECKPOINT(0x04),

    /**
     * Specifies that this snapshot marker should return a response once the entire snapshot is received.
     *
     * To acknowledge {@link DcpSnapshotMarkerResponse} have to be sent back with the same opaque value.
     */
    ACK(0x08);

    private final int value;

    SnapshotMarkerFlags(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public boolean isSet(int flags) {
        return (flags & value) == value;
    }
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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

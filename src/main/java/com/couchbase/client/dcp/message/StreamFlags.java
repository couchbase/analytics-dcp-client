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
 * Flags, that could be used when initiating new vBucket stream.
 */
public enum StreamFlags {
    /**
     * Specifies that the stream should send over all remaining data to the remote node and
     * then set the remote nodes VBucket to active state and the source nodes VBucket to dead.
     */
    TAKEOVER(0x01),
    /**
     * Specifies that the stream should only send items only if they are on disk. The first
     * item sent is specified by the start sequence number and items will be sent up to the
     * sequence number specified by the end sequence number or the last on disk item when
     * the stream is created.
     */
    DISK_ONLY(0x02),
    /**
     * Specifies that the server should stream all mutations up to the current sequence number
     * for that VBucket. The server will overwrite the value of the end sequence number field
     * with the value of the latest sequence number.
     */
    LATEST(0x04),
    /**
     * Specifies that the server should stream only item key and metadata in the mutations
     * and not stream the value of the item.
     */
    NO_VALUE(0x08),
    /**
     * Indicates if the server is collection capable.
     */
    FEATURES_COLLECTION(0x12),
    /**
     * Indicate the server to add stream only if the VBucket is active.
     * If the VBucket is not active, the stream request fails with ERR_NOT_MY_VBUCKET (0x07)
     */
    ACTIVE_VB_ONLY(0x16);

    private final int value;

    StreamFlags(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public boolean isSet(int flags) {
        return (flags & value) == value;
    }
}

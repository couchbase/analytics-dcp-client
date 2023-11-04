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

import com.couchbase.client.core.annotation.SinceCouchbase;

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
     * Indicate the server to add stream only if the VBucket is active.
     * If the VBucket is not active, the stream request fails with ERR_NOT_MY_VBUCKET (0x07).
     */
    @SinceCouchbase("5.0") ACTIVE_VB_ONLY(0x10),
    /**
     * Specifies that the server should check for vb_uuid match even at start_seqno 0 before
     * adding the stream. Upon mismatch the sever should return ENGINE_ROLLBACK error.
     */
    @SinceCouchbase("5.1") STRICT_VBUUID_MATCH(0x20),
    /**
     * Specifies that the server should stream mutations from the current sequence number, this
     * means the start parameter is ignored.
     */
    @SinceCouchbase("7.2") FROM_LATEST(0x40),
    /**
     * Specifies that the server should skip rollback if the client is behind the purge seqno, but
     * the request is otherwise satisfiable (i.e. no other rollback checks such as UUID mismatch
     * fail). The client could end up missing purged tombstones (and hence could end up never being
     * told about a document deletion). The intent of this flag is to allow clients who ignore
     * deletes to avoid rollbacks to zero which are solely due to them being behind the purge seqno.
     */
    @SinceCouchbase("7.2") IGNORE_PURGED_TOMBSTONES(0x80);
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

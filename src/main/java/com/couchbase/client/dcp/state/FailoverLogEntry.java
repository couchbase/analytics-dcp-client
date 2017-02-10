/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a single entry in a failover log per partition state.
 *
 * @since 1.0.0
 * @author Michael Nitschinger
 */
public class FailoverLogEntry {

    private final long seqno;

    private final long uuid;

    @JsonCreator
    public FailoverLogEntry(@JsonProperty("seqno") long seqno, @JsonProperty("uuid") long uuid) {
        this.seqno = seqno;
        this.uuid = uuid;
    }

    public long getSeqno() {
        return seqno;
    }

    public long getUuid() {
        return uuid;
    }

    @Override
    public String toString() {
        return "FailoverLogEntry{" + "seqno=" + seqno + ", uuid=" + uuid + '}';
    }
}

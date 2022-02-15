/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.state;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;

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
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(toMap());
        } catch (IOException e) {
            return "{\"object\"=\"failed\"}";
        }
    }

    public Map<String, Object> toMap() {
        Map<String, Object> tree = new HashMap<>();
        tree.put("uuid", uuid);
        tree.put("seqno", seqno);
        return tree;
    }
}

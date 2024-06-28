/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.util.MemcachedStatus;

public class OpenStreamRollbackResponse extends OpenStreamResponse {
    private final long rollbackSeq;

    public OpenStreamRollbackResponse(StreamPartitionState state, long rollbackSeq) {
        super(state, MemcachedStatus.ROLLBACK);
        this.rollbackSeq = rollbackSeq;
    }

    @Override
    public Type getType() {
        return Type.OPEN_STREAM_ROLLBACK_RESPONSE;
    }

    public long getRollbackSeq() {
        return rollbackSeq;
    }

    public boolean isFullRollback() {
        return rollbackSeq == 0;
    }

    @Override
    public String toString() {
        return "{\"sid\":" + state.getStreamState().streamId() + ",\"vbucket\":" + state.vbid()
                + ",\"open-stream-response\":\"" + MemcachedStatus.toString(status) + "\"," + "\"rollback-seq\":"
                + Long.toUnsignedString(rollbackSeq) + "}";
    }
}

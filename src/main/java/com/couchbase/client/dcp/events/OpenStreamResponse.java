/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package com.couchbase.client.dcp.events;

import org.apache.hyracks.util.Span;

import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.util.MemcachedStatus;

public class OpenStreamResponse implements PartitionDcpEvent {
    protected final StreamPartitionState state;
    protected final short status;

    public OpenStreamResponse(StreamPartitionState state, short status) {
        this.state = state;
        this.status = status;
        state.calculateNextDelay(status);
    }

    @Override
    public Type getType() {
        return Type.OPEN_STREAM_RESPONSE;
    }

    public short getStatus() {
        return status;
    }

    @Override
    public StreamPartitionState getPartitionState() {
        return state;
    }

    @Override
    public String toString() {
        return "{\"sid\":" + state.getStreamState().streamId() + ",\"vbucket\":" + state.vbid()
                + ",\"open-stream-response\":\"" + MemcachedStatus.toString(status) + "}";
    }

    @Override
    public Span delay() {
        return state.getDelay();
    }
}

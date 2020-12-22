/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.state.StreamState;

/**
 * Event published when stream has stopped activity.
 */
public class StreamEndEvent implements PartitionDcpEvent {
    private final StreamPartitionState state;
    private final StreamState stream;
    private StreamEndReason reason;
    private boolean failoverLogsRequested;
    private boolean seqRequested;
    private int attempts = 0;

    public StreamEndEvent(StreamPartitionState state, StreamState stream, StreamEndReason reason) {
        this.state = state;
        this.stream = stream;
        this.reason = reason;
    }

    @Override
    public Type getType() {
        return Type.STREAM_END;
    }

    public short partition() {
        return state.vbid();
    }

    public StreamEndReason reason() {
        return reason;
    }

    public void reset() {
        reason = StreamEndReason.UNKNOWN;
        setFailoverLogsRequested(false);
        setSeqRequested(false);
        attempts = 0;
    }

    public void setReason(StreamEndReason reason) {
        this.reason = reason;
    }

    public StreamState getStreamState() {
        return stream;
    }

    @Override
    public String toString() {
        return "StreamEndEvent{" + "streamId=" + stream.streamId() + " partition=" + state.vbid() + " reason=" + reason
                + '}';
    }

    public StreamPartitionState getState() {
        return state;
    }

    public boolean isFailoverLogsRequested() {
        return failoverLogsRequested;
    }

    public void setFailoverLogsRequested(boolean failoverLogsRequested) {
        this.failoverLogsRequested = failoverLogsRequested;
    }

    public boolean isSeqRequested() {
        return seqRequested;
    }

    public void setSeqRequested(boolean seqRequested) {
        this.seqRequested = seqRequested;
    }

    public void incrementAttempts() {
        attempts++;
    }

    public int getAttempts() {
        return attempts;
    }

    @Override
    public StreamPartitionState getPartitionState() {
        return state;
    }
}

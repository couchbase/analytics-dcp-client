package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.util.MemcachedStatus;

public class OpenStreamResponse implements PartitionDcpEvent {
    private final PartitionState state;
    private DcpChannel channel;
    private short status;
    private long rollbackSeq;

    public OpenStreamResponse(PartitionState state) {
        this.state = state;
    }

    @Override
    public Type getType() {
        return Type.OPEN_STREAM_RESPONSE;
    }

    public void setStatus(short status) {
        this.status = status;
    }

    public short getStatus() {
        return status;
    }

    @Override
    public PartitionState getPartitionState() {
        return state;
    }

    public long getRollbackSeq() {
        return rollbackSeq;
    }

    public void setRollbackSeq(long rollbackSeq) {
        this.rollbackSeq = rollbackSeq;
    }

    public void setChannel(DcpChannel channel) {
        this.channel = channel;
    }

    public DcpChannel getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "{\"open-stream-response\":\"" + MemcachedStatus.toString(status) + "\"}";
    }
}

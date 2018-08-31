package com.couchbase.client.dcp.events;

import java.util.concurrent.TimeUnit;

import org.apache.hyracks.util.Span;

import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.util.MemcachedStatus;

public class OpenStreamResponse implements PartitionDcpEvent {
    private final PartitionState state;
    private DcpChannel channel;
    private short status;
    private long rollbackSeq;
    private Span delay = ELAPSED;

    public OpenStreamResponse(PartitionState state) {
        this.state = state;
    }

    @Override
    public Type getType() {
        return Type.OPEN_STREAM_RESPONSE;
    }

    public void setStatus(short status) {
        this.status = status;
        if (status == MemcachedStatus.SUCCESS) {
            delay = ELAPSED;
        } else {
            calculateNextDelay();
        }
    }

    private void calculateNextDelay() {
        if (delay.getSpanNanos() == 0) {
            // start with 1s
            delay = Span.start(1, TimeUnit.SECONDS);
        } else {
            // double the delay, capping at 64s
            delay = Span.start(Long.min(delay.getSpanNanos() * 2, TimeUnit.SECONDS.toNanos(64)), TimeUnit.NANOSECONDS);
        }
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

    @Override
    public Span delay() {
        return delay;
    }
}

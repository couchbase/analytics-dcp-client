package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.state.PartitionState;

public class NotMyVBucketEvent implements DcpEvent {

    private DcpChannel channel;
    private final PartitionState state;

    public NotMyVBucketEvent(PartitionState state) {
        this.state = state;
    }

    public DcpChannel getChannel() {
        return channel;
    }

    public void setChannel(DcpChannel channel) {
        this.channel = channel;
    }

    public short getVbid() {
        return state.vbid();
    }

    public PartitionState getPartitionState() {
        return state;
    }

    @Override
    public Type getType() {
        return Type.NOT_MY_VBUCKET;
    }
}

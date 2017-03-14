package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.state.PartitionState;

public class FailoverLogUpdateEvent implements DcpEvent {

    private final PartitionState partitionState;

    public FailoverLogUpdateEvent(PartitionState partitionState) {
        this.partitionState = partitionState;
    }

    @Override
    public Type getType() {
        return Type.FAILOVER_UPDATE;
    }

    public PartitionState getPartitionState() {
        return partitionState;
    }

}

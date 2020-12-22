package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.state.StreamPartitionState;

public interface PartitionDcpEvent extends DcpEvent {

    StreamPartitionState getPartitionState();
}

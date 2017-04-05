package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.state.PartitionState;

public interface PartitionDcpEvent extends DcpEvent {

    PartitionState getPartitionState();

}

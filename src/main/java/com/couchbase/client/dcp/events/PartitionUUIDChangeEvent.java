/*
 * Copyright (c) 2018 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.state.PartitionState;

public class PartitionUUIDChangeEvent implements PartitionDcpEvent {

    private final PartitionState ps;

    public PartitionUUIDChangeEvent(PartitionState ps) {
        this.ps = ps;
    }

    @Override
    public Type getType() {
        return Type.PARTITION_UUID_CHANGED;
    }

    @Override
    public PartitionState getPartitionState() {
        return ps;
    }

}

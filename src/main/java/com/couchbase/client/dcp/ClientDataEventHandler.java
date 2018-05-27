/*
 * Copyright (c) 2018 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

import com.couchbase.client.dcp.events.PartitionUUIDChangeEvent;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public interface ClientDataEventHandler {
    /**
     * Called when a data event happens.
     *
     * Make sure to release the buffers!!
     *
     * @param ackHandle
     *            a handle to use when ack-ing
     * @param event
     *            the data event happening right now.
     * @param uuidChange
     *            a uuid change event if a failover log boundary has been crossed, null otherwise
     */
    void onEvent(DcpAckHandle ackHandle, ByteBuf event, PartitionUUIDChangeEvent uuidChange);
}

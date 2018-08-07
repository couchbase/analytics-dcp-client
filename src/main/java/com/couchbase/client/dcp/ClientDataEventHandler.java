/*
 * Copyright (c) 2018 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

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
     */
    void onEvent(DcpAckHandle ackHandle, ByteBuf event);
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

/**
 * This interface acts as a callback on the {@link Client#dataEventHandler(DataEventHandler)} API
 * that allows one to react to data events.
 *
 * Right now {@link com.couchbase.client.dcp.message.DcpMutationMessage},
 * {@link com.couchbase.client.dcp.message.DcpExpirationMessage} and
 * {@link com.couchbase.client.dcp.message.DcpDeletionMessage} are emitted, but the expirations are
 * as of Couchbase Server 4.5.0 not actually emitted. So while good practice to handle them, even if
 * you opt out to do so make sure to release the buffers.
 *
 * Keep in mind that the callback is called on the IO event loops, so you should never block or run
 * expensive computations in the callback! Use queues and other synchronization primities!
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
@FunctionalInterface
public interface DataEventHandler {

    /**
     * Called when a data event happens.
     *
     * Make sure to release the buffers!!
     *
     * @param event
     *            the data event happening right now.
     */
    void onEvent(DcpAckHandle ackHandle, ByteBuf event);

}

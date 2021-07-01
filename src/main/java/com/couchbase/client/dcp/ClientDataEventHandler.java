/*
 * Copyright 2018-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
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

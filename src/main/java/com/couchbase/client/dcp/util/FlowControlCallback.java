/*
 * Copyright 2017-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.util;

import com.couchbase.client.dcp.DcpAckHandle;
import com.couchbase.client.dcp.conductor.DcpChannel;

public interface FlowControlCallback {

    final FlowControlCallback NOOP = new FlowControlCallback() {
        @Override
        public void bufferAckWaterMarkReached(DcpAckHandle handle, DcpChannel dcpChannel, int ackCounter,
                long ackWatermark) {
            // No Op
        }

        @Override
        public void ackFlushedThroughNetwork(DcpAckHandle handle, DcpChannel dcpChannel) {
            // No Op
        }
    };

    void bufferAckWaterMarkReached(DcpAckHandle handle, DcpChannel dcpChannel, int ackCounter, long ackWatermark);

    void ackFlushedThroughNetwork(DcpAckHandle handle, DcpChannel dcpChannel);

}

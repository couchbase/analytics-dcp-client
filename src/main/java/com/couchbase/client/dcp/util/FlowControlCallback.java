/*
 * Copyright (c) 2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.util;

import com.couchbase.client.dcp.DcpAckHandle;
import com.couchbase.client.dcp.conductor.DcpChannel;

public interface FlowControlCallback {

    final FlowControlCallback NOOP = new FlowControlCallback() {
        @Override
        public void bufferAckWaterMarkReached(DcpAckHandle handle, DcpChannel dcpChannel, int ackCounter,
                int ackWatermark) {
            // No Op
        }

        @Override
        public void ackFlushedThroughNetwork(DcpAckHandle handle, DcpChannel dcpChannel) {
            // No Op
        }
    };

    void bufferAckWaterMarkReached(DcpAckHandle handle, DcpChannel dcpChannel, int ackCounter, int ackWatermark);

    void ackFlushedThroughNetwork(DcpAckHandle handle, DcpChannel dcpChannel);

}

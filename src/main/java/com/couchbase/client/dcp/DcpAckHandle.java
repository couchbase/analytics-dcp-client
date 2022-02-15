/*
 * Copyright 2017-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

@FunctionalInterface
public interface DcpAckHandle {
    void ack(ByteBuf message);

    class Util {
        public static final DcpAckHandle NOOP_ACK_HANDLE = message -> {
        };

        private Util() {
            throw new AssertionError("do not instantiate");
        }
    }
}

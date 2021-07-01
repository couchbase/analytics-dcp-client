/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.GET_ALL_VB_SEQNOS_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpGetPartitionSeqnosResponse {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == GET_ALL_VB_SEQNOS_OPCODE;
    }

    public static int numPairs(final ByteBuf buffer) {
        int bodyLength = MessageUtil.getContent(buffer).readableBytes();
        return bodyLength / 10; // one pair is short + long = 10 bytes
    }
}

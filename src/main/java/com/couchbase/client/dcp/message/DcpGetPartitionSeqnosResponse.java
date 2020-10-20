/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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

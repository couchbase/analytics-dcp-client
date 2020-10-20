/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.GET_ALL_VB_SEQNOS_OPCODE;

import java.util.Arrays;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

public enum DcpGetPartitionSeqnosRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == GET_ALL_VB_SEQNOS_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(GET_ALL_VB_SEQNOS_OPCODE, buffer);
    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        MessageUtil.setOpaque(opaque, buffer);
    }

    public static void vbucketStateAndCid(final ByteBuf buffer, VbucketState vbucketState, int... cids) {
        if (cids.length > 1) {
            throw new IllegalArgumentException(
                    "at most one collection id can be specified, but was " + Arrays.toString(cids));
        }
        switch (vbucketState) {
            case ANY:
            case ACTIVE:
            case REPLICA:
            case PENDING:
            case DEAD:
                ByteBuf extras = Unpooled.buffer(cids.length == 0 ? 4 : 8);
                extras.writeInt(vbucketState.value());
                if (cids.length == 1) {
                    extras.writeInt(cids[0]);
                }
                MessageUtil.setExtras(extras, buffer);
                extras.release();
                break;
            default:
                throw new IllegalStateException("nyi: " + vbucketState);
        }
    }

}

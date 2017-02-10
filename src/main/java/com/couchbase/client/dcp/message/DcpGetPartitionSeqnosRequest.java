/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.GET_SEQNOS_OPCODE;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

public enum DcpGetPartitionSeqnosRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == GET_SEQNOS_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(GET_SEQNOS_OPCODE, buffer);
    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        MessageUtil.setOpaque(opaque, buffer);
    }

    public static void vbucketState(final ByteBuf buffer, VbucketState vbucketState) {

        switch (vbucketState) {
            case ANY:
                break;
            case ACTIVE:
            case REPLICA:
            case PENDING:
            case DEAD:
                ByteBuf extras = Unpooled.buffer(4);
                MessageUtil.setExtras(extras.writeInt(vbucketState.value()), buffer);
                extras.release();
        }
    }

}

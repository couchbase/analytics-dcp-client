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
import static com.couchbase.client.dcp.message.MessageUtil.GET_SEQNOS_GLOBAL_COLLECTION_ID;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

public enum DcpGetPartitionSeqnosRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == GET_ALL_VB_SEQNOS_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(GET_ALL_VB_SEQNOS_OPCODE, buffer);
    }

    public static void vbucketStateAndCid(final ByteBuf buffer, VbucketState vbucketState, int cid,
            boolean collectionCapable) {

        MessageUtil.setOpaque(cid, buffer);
        boolean encodeCid = collectionCapable && cid != GET_SEQNOS_GLOBAL_COLLECTION_ID;
        switch (vbucketState) {
            case ANY:
            case ACTIVE:
            case REPLICA:
            case PENDING:
            case DEAD:
                ByteBuf extras = Unpooled.buffer(encodeCid ? 8 : 4);
                extras.writeInt(vbucketState.value());
                if (encodeCid) {
                    extras.writeInt(cid);
                }
                MessageUtil.setExtras(extras, buffer);
                extras.release();
                break;
            default:
                throw new IllegalStateException("nyi: " + vbucketState);
        }
    }
}

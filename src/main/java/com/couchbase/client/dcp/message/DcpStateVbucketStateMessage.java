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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_SET_VBUCKET_STATE_OPCODE;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

/**
 * This message is used during the VBucket takeover process to hand off ownership of a VBucket between two nodes.
 */
public enum DcpStateVbucketStateMessage {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_SET_VBUCKET_STATE_OPCODE;
    }

    public static int flags(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getInt(0);
    }

    public static boolean active(final ByteBuf buffer) {
        return (flags(buffer) & State.ACTIVE.value) == State.ACTIVE.value;
    }

    public static boolean replica(final ByteBuf buffer) {
        return (flags(buffer) & State.REPLICA.value) == State.REPLICA.value;
    }

    public static boolean pending(final ByteBuf buffer) {
        return (flags(buffer) & State.PENDING.value) == State.PENDING.value;
    }

    public static boolean dead(final ByteBuf buffer) {
        return (flags(buffer) & State.DEAD.value) == State.DEAD.value;
    }

    public static void init(final ByteBuf buffer, State state) {
        MessageUtil.initRequest(DCP_SET_VBUCKET_STATE_OPCODE, buffer);
        state(buffer, state);
    }

    public static void state(final ByteBuf buffer, State state) {
        ByteBuf extras = Unpooled.buffer(4);
        MessageUtil.setExtras(extras.writeInt(state.value), buffer);
        extras.release();
    }

    public static State state(final ByteBuf buffer) {
        return State.of(MessageUtil.getExtras(buffer).getInt(0));
    }

    public enum State {
        ACTIVE(0x01),
        REPLICA(0x02),
        PENDING(0x03),
        DEAD(0x04);

        private final int value;

        State(int value) {
            this.value = value;
        }

        static State of(int value) {
            switch (value) {
                case 0x01:
                    return ACTIVE;
                case 0x02:
                    return REPLICA;
                case 0x03:
                    return PENDING;
                case 0x04:
                    return DEAD;
                default:
                    throw new IllegalArgumentException("Unknown VBucket state: " + value);
            }
        }
    }
}

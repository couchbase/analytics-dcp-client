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

import static com.couchbase.client.dcp.message.MessageUtil.OPEN_CONNECTION_OPCODE;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

public enum DcpOpenConnectionRequest {
    ;

    /**
     * If the given buffer is a {@link DcpOpenConnectionRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == OPEN_CONNECTION_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     *
     * Note that this will implicitly set the flags to "consumer".
     */
    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(OPEN_CONNECTION_OPCODE, buffer);
        ByteBuf extras = Unpooled.buffer(8);
        MessageUtil.setExtras(extras.writeInt(0).writeInt(Type.PRODUCER.value), buffer);
        extras.release();
    }

    /**
     * Set the connection name on the buffer.
     */
    public static void connectionName(final ByteBuf buffer, final ByteBuf connectionName) {
        MessageUtil.setKey(connectionName, buffer);
    }

    enum Type {
        /**
         * Consumer type of DCP connection is set then the sender of the {@link DcpOpenConnectionRequest}
         * will be a Producer
         */
        CONSUMER(0x00),
        /**
         * Producer type of DCP connection is set then the sender of the {@link DcpOpenConnectionRequest}
         * will be a Consumer
         */
        PRODUCER(0x01),
        /**
         * Notifier type of DCP connection is set then the sender of the {@link DcpOpenConnectionRequest}
         * will be a Consumer. Notifier mode is almost the same as Producer, but it relaxes some of the
         * checks when adding streams. It does not check provided sequence numbers and always streams
         * vbucket from highest sequence number.
         */
        NOTIFIER(0x02);

        private final int value;

        Type(int value) {
            this.value = value;
        }
    }
}

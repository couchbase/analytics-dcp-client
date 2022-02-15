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

import static com.couchbase.client.dcp.message.MessageUtil.SASL_LIST_MECHS_OPCODE;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;

public enum SaslListMechsResponse {
    ;

    /**
     * If the given buffer is a {@link SaslListMechsResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == SASL_LIST_MECHS_OPCODE;
    }

    /**
     * Extracts the supported SASL mechanisms as a string array.
     *
     * @param buffer
     *            the buffer to extract from.
     * @return the array of supported mechs, or an empty array if none found.
     */
    public static String[] supportedMechs(final ByteBuf buffer) {
        int bodyLength = buffer.getInt(MessageUtil.BODY_LENGTH_OFFSET);
        ByteBuf contentSlice = buffer.slice(MessageUtil.getHeaderSize(buffer), bodyLength);
        String content = contentSlice.toString(CharsetUtil.UTF_8);
        if (content == null) {
            return new String[] {};
        }
        return content.split(" ");
    }

}

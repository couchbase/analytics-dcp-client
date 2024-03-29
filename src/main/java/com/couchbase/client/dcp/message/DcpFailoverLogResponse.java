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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_FAILOVER_LOG_OPCODE;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.state.SessionPartitionState;

public enum DcpFailoverLogResponse {
    ;

    private static final Logger LOGGER = LogManager.getLogger();

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_FAILOVER_LOG_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initResponse(DCP_FAILOVER_LOG_OPCODE, buffer);
    }

    public static void vbucket(final ByteBuf buffer, final short vbid) {
        MessageUtil.setVbucket(vbid, buffer);
    }

    public static short vbucket(final ByteBuf buffer) {
        int vbOffset = MessageUtil.getContent(buffer).readableBytes() - 2;
        return MessageUtil.getContent(buffer).getShort(vbOffset);
    }

    public static int numLogEntries(final ByteBuf buffer) {
        return (MessageUtil.getContent(buffer).readableBytes() - 2) / 16;
    }

    public static long vbuuidEntry(final ByteBuf buffer, int index) {
        return MessageUtil.getContent(buffer).getLong(index * 16);
    }

    public static long seqnoEntry(final ByteBuf buffer, int index) {
        return MessageUtil.getContent(buffer).getLong(index * 16 + 8);
    }

    public static String toString(final ByteBuf buffer) {
        StringBuilder sb = new StringBuilder();
        sb.append("FailoverLog [");
        sb.append("vbid: ").append(vbucket(buffer)).append(", log: [");
        int numEntries = numLogEntries(buffer);
        for (int i = 0; i < numEntries; i++) {
            sb.append("[uuid: ").append(vbuuidEntry(buffer, i)).append(", seqno: ").append(seqnoEntry(buffer, i))
                    .append("]");
        }
        return sb.append("]]").toString();
    }

    public static void fill(final ByteBuf buffer, SessionPartitionState ss) {
        ss.clearFailoverLog();
        int numEntries = numLogEntries(buffer);
        if (LOGGER.isEnabled(Level.TRACE)) {
            LOGGER.trace("Failover log response for vbucket " + ss.vbid() + " contains " + numEntries + " entries");
        }
        for (int i = numEntries - 1; i >= 0; i--) {
            long seq = seqnoEntry(buffer, i);
            long uuid = vbuuidEntry(buffer, i);
            ss.addToFailoverLog(seq, uuid);
        }
    }
}

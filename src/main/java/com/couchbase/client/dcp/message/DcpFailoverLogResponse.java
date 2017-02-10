/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_FAILOVER_LOG_OPCODE;

import java.util.List;

import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum DcpFailoverLogResponse {
    ;

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

    public static void fill(final ByteBuf buffer, List<FailoverLogEntry> logs) {
        int numEntries = numLogEntries(buffer);
        for (int i = 0; i < numEntries; i++) {
            logs.add(new FailoverLogEntry(seqnoEntry(buffer, i), vbuuidEntry(buffer, i)));
        }
    }
}

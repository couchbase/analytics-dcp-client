/*
 * Copyright (c) 2016-2021 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import static com.couchbase.client.dcp.transport.netty.Stat.Kind.COLLECTIONS_BYID;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.CollectionsUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

public class Stat {

    public enum Kind {
        UNKNOWN,
        COLLECTIONS_BYID;

        public static Kind valueOf(int ordinal, Kind defaultValue) {
            return ordinal < values().length ? values()[ordinal] : defaultValue;
        }

        @Override
        public String toString() {
            return name().toLowerCase().replace('_', '-');
        }
    }

    private Stat() {
    }

    public static void init(ByteBuf buffer) {
        MessageUtil.initRequest(MessageUtil.STAT_OPCODE, buffer);
    }

    public static void collectionsById(ByteBuf buffer, int cid) {
        ByteBuf key = Unpooled.copiedBuffer("collections-byid " + CollectionsUtil.encodeCid(cid), CharsetUtil.UTF_8);
        MessageUtil.setKey(key, buffer);
        MessageUtil.setOpaque(COLLECTIONS_BYID.ordinal(), buffer);
    }
}

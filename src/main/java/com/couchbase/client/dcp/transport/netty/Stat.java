/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.transport.netty;

import static com.couchbase.client.dcp.transport.netty.Stat.Kind.COLLECTIONS_BYID;

import java.util.Collections;
import java.util.Map;

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

    public enum CollectionsByid {
        UNKNOWN,
        ITEMS;

        private static final Map<String, CollectionsByid> nameMap = Collections.singletonMap("items", ITEMS);

        public static CollectionsByid parseStatParts(String[] parts) {
            if (parts.length == 3) {
                return nameMap.getOrDefault(parts[2], UNKNOWN);
            }
            return UNKNOWN;
        }
    }

    private Stat() {
    }

    public static void init(ByteBuf buffer) {
        MessageUtil.initRequest(MessageUtil.STAT_OPCODE, buffer);
    }

    public static void collectionsById(ByteBuf buffer, int cid) {
        ByteBuf key = Unpooled.copiedBuffer(COLLECTIONS_BYID + " " + CollectionsUtil.encodeCid(cid), CharsetUtil.UTF_8);
        MessageUtil.setKey(key, buffer);
        MessageUtil.setOpaque(COLLECTIONS_BYID.ordinal(), buffer);
    }
}

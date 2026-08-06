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
import static com.couchbase.client.dcp.transport.netty.Stat.Kind.CURR_ITEMS;

import java.util.Collections;
import java.util.Map;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.CollectionsUtil;

public class Stat {

    public enum Kind {
        UNKNOWN,
        COLLECTIONS_BYID,
        CURR_ITEMS;

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

    private static void initCommon(ByteBuf buffer) {
        MessageUtil.initRequest(MessageUtil.STAT_OPCODE, buffer);
    }

    public static void initCollectionsById(ByteBuf buffer, int cid) {
        initCommon(buffer);
        ByteBuf key = Unpooled.copiedBuffer(COLLECTIONS_BYID + " " + CollectionsUtil.encodeCid(cid), CharsetUtil.UTF_8);
        MessageUtil.setKey(key, buffer);
        MessageUtil.setOpaque(COLLECTIONS_BYID.ordinal(), buffer);
    }

    public static void initCurrItems(ByteBuf buffer) {
        initCommon(buffer);
        MessageUtil.setOpaque(CURR_ITEMS.ordinal(), buffer);
    }
}

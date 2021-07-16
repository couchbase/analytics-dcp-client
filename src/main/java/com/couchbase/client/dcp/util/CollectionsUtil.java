/*
 * Copyright 2021-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CollectionsUtil {
    private CollectionsUtil() {
        throw new AssertionError("do not instantiate");
    }

    public static String encodeCid(int cid) {
        return Integer.toUnsignedString(cid, 16);
    }

    public static int decodeCid(String cid) {
        return Integer.parseUnsignedInt(cid, 16);
    }

    public static String displayCid(int cid) {
        return "0x" + encodeCid(cid);
    }

    public static List<String> displayCids(int... cids) {
        return IntStream.of(cids).mapToObj(CollectionsUtil::displayCid).collect(Collectors.toList());
    }

    public static String encodeManifestUid(long uid) {
        return Long.toUnsignedString(uid, 16);
    }

    public static long decodeManifestUid(String uid) {
        return Long.parseUnsignedLong(uid, 16);
    }

    public static String displayManifestUid(long uid) {
        return "0x" + encodeManifestUid(uid);
    }
}

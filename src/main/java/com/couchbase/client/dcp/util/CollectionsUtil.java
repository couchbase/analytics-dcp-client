/*
 * Copyright 2021 Couchbase, Inc.
 */
package com.couchbase.client.dcp.util;

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

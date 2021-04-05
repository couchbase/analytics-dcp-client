/*
 * Copyright 2020 Couchbase, Inc.
 */
package com.couchbase.client.dcp.util;

import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;

public class MathUtil {
    private static final Long2ByteMap LOG2_MAP;

    static {
        LOG2_MAP = new Long2ByteOpenHashMap();
        for (byte i = 0; i < Long.SIZE; i++) {
            com.couchbase.client.dcp.util.MathUtil.LOG2_MAP.put(1L << i, i);
        }
    }

    private MathUtil() {
        throw new AssertionError("do not instantiate");
    }

    public static long maxUnsigned(long a, long b) {
        return Long.compareUnsigned(a, b) > 0 ? a : b;
    }

    public static long minUnsigned(long a, long b) {
        return Long.compareUnsigned(a, b) < 0 ? a : b;
    }

    public static long log2Unsigned(long value) {
        final byte result = LOG2_MAP.getOrDefault(value, Byte.MIN_VALUE);
        if (result < 0) {
            throw new IllegalArgumentException("cannot resolve log2 value for " + Long.toUnsignedString(value, 16));
        }
        return result;
    }
}

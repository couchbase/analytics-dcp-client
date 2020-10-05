/*
 * Copyright 2020 Couchbase, Inc.
 */
package com.couchbase.client.dcp.util;

public class MathUtil {
    private MathUtil() {
        throw new AssertionError("do not instantiate");
    }

    public static long maxUnsigned(long a, long b) {
        return Long.compareUnsigned(a, b) > 0 ? a : b;
    }

    public static long minUnsigned(long a, long b) {
        return Long.compareUnsigned(a, b) < 0 ? a : b;
    }
}

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

import java.util.NoSuchElementException;

import it.unimi.dsi.fastutil.shorts.ShortIterator;

public class ShortUtil {
    private ShortUtil() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Returns a compact string representation of the supplied {@link ShortIterator}, enclosed
     * by square-brackets ([]), comma-delimited, collapsing ranges together with a hyphen (-).
     * Only provides reasonable results if the contents of the iterator are sorted.
     */
    public static String toCompactString(ShortIterator iter) {
        if (!iter.hasNext()) {
            return "[]";
        }
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        appendCompact(iter, builder);
        builder.append(']');
        return builder.toString();
    }

    /**
     * Returns a compact string representation of the supplied short array, enclosed
     * by square-brackets ([]), comma-delimited, collapsing ranges together with a hyphen (-).
     * Only provides reasonable results if the contents of the iterator are sorted.
     */
    public static String toCompactString(short[] iter) {
        return toCompactString(wrap(iter));
    }

    /**
     * Appends the contents of the supplied {@link ShortIterator} to the {@link StringBuilder} instance,
     * comma-delimited, collapsing ranges together with a hyphen (-).  Only provides reasonable
     * results if the contents of the iterator are sorted.
     */
    public static void appendCompact(ShortIterator iter, StringBuilder builder) {
        short rangeStart = iter.nextShort();
        builder.append(rangeStart);
        short current = rangeStart;
        short prev = current;
        while (iter.hasNext()) {
            current = iter.nextShort();
            if (current != prev + 1) {
                // end any range we were in:
                if (rangeStart != prev) {
                    builder.append('-').append(prev);
                }
                builder.append(",").append(current);
                rangeStart = current;
            }
            prev = current;
        }
        if (rangeStart != prev) {
            builder.append('-').append(prev);
        }
    }

    public static ShortIterator wrap(short... vbuckets) {
        return new ShortIterator() {
            int index = 0;

            @Override
            public short nextShort() {
                try {
                    return vbuckets[index++];
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public boolean hasNext() {
                return index < vbuckets.length;
            }
        };
    }
}

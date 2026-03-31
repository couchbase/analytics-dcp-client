/*
 * Copyright 2025-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.util;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.shorts.Short2ObjectFunction;
import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ShortFunction;
import it.unimi.dsi.fastutil.shorts.ShortIterator;
import it.unimi.dsi.fastutil.shorts.ShortSortedSet;

public class DcpUtil {

    private DcpUtil() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Returns the max seqno, where any valid seqno is considered higher than INVALID_SEQNO
     */
    public static long maxSeqNo(long a, long b) {
        return MathUtil.maxUnsigned(a + 1, b + 1) - 1;
    }

    public static long minSeqNo(long a, long b) {
        return MathUtil.minUnsigned(a, b);
    }

    public static <T> void consolidateAppendVbucketStates(int count, Short2ShortFunction vbucketFunction,
            Short2ObjectFunction<T> stateFunction, Function<T, ?> displayFunction, StringBuilder builder) {
        ShortSortedSet matchingVbids = new ShortSortedBitSet();

        short index = 0;
        short startingVbid = -1;
        T nextState = null;
        while (index < count) {
            if (index != 0) {
                builder.append(",");
            } else {
                startingVbid = vbucketFunction.get(index);
            }
            matchingVbids.clear();
            matchingVbids.add(startingVbid); // starting vbucket in segment
            T startingState = stateFunction.get(index++);
            short lastVbid = startingVbid;
            startingVbid = -1;
            while (index < count) {
                short j = vbucketFunction.get(index);
                nextState = stateFunction.get(index);
                if (j == (lastVbid + 1) && Objects.equals(startingState, nextState)) {
                    matchingVbids.add(j);
                    lastVbid = j;
                    index++;
                } else {
                    startingVbid = j;
                    break;
                }
            }
            builder.append('"');
            ShortUtil.appendCompact(matchingVbids.iterator(), builder);
            builder.append(":").append(displayFunction.apply(startingState)).append('"');
        }
        if (startingVbid != -1) {
            builder.append(",\"").append(startingVbid).append(":").append(displayFunction.apply(nextState)).append('"');
        }
    }

    public static <T> void consolidateAppendVbucketStates(ShortIterator vbuckets, Short2ObjectFunction<T> stateFunction,
            Function<T, ?> displayFunction, StringBuilder builder) {
        ShortSortedSet matchingVbids = new ShortSortedBitSet();

        boolean first = true;
        short startingVbid = -1;
        T nextState = null;
        while (vbuckets.hasNext()) {
            if (!first) {
                builder.append(",");
            } else {
                first = false;
                startingVbid = vbuckets.nextShort();
            }
            matchingVbids.clear();
            matchingVbids.add(startingVbid);
            T startingState = stateFunction.get(startingVbid);
            short lastVbid = startingVbid;
            startingVbid = -1;
            while (vbuckets.hasNext()) {
                short j = vbuckets.nextShort();
                nextState = stateFunction.get(j);
                if (j == (lastVbid + 1) && Objects.equals(startingState, nextState)) {
                    matchingVbids.add(j);
                    startingVbid = -1;
                    lastVbid = j;
                } else {
                    startingVbid = j;
                    break;
                }
            }
            builder.append('"');
            ShortUtil.appendCompact(matchingVbids.iterator(), builder);
            builder.append(":").append(displayFunction.apply(startingState)).append('"');
        }
        if (startingVbid != -1) {
            builder.append(",\"").append(startingVbid).append(":").append(displayFunction.apply(nextState)).append('"');
        }
    }

    public static <T> void consolidateAppendVbucketStates(ShortIterator vbuckets, Iterator<T> states,
            Function<T, ?> displayFunction, StringBuilder builder) {
        ShortSortedSet matchingVbids = new ShortSortedBitSet();

        short startingVbid = -1;
        boolean first = true;
        T nextState = null;
        T startingState = null;
        while (vbuckets.hasNext()) {
            if (!first) {
                builder.append(",");
            } else {
                first = false;
                startingVbid = vbuckets.nextShort();
                startingState = states.next();
            }
            matchingVbids.clear();
            matchingVbids.add(startingVbid);
            short lastVbid = startingVbid;
            startingVbid = -1;
            while (vbuckets.hasNext()) {
                short j = vbuckets.nextShort();
                nextState = states.next();
                if (j == (lastVbid + 1) && Objects.equals(startingState, nextState)) {
                    matchingVbids.add(j);
                    startingVbid = -1;
                    lastVbid = j;
                } else {
                    startingVbid = j;
                    break;
                }
            }
            builder.append('"');
            ShortUtil.appendCompact(matchingVbids.iterator(), builder);
            builder.append(":").append(displayFunction.apply(startingState)).append('"');
            startingState = nextState;
        }
        if (startingVbid != -1) {
            builder.append(",\"").append(startingVbid).append(":").append(displayFunction.apply(nextState)).append('"');
        }
    }

    public static <T> void consolidateAppendVbucketStates(ObjectIterator<Short2ObjectMap.Entry<T>> vbucketStates,
            Function<T, ?> displayFunction, StringBuilder builder) {
        ShortSortedSet matchingVbids = new ShortSortedBitSet();

        short startingVbid = -1;
        boolean first = true;
        T nextState = null;
        T startingState = null;
        while (vbucketStates.hasNext()) {
            if (!first) {
                builder.append(",");
            } else {
                first = false;
                Short2ObjectMap.Entry<T> entry = vbucketStates.next();
                startingVbid = entry.getShortKey();
                startingState = entry.getValue();
            }
            matchingVbids.clear();
            matchingVbids.add(startingVbid);
            short lastVbid = startingVbid;
            startingVbid = -1;
            while (vbucketStates.hasNext()) {
                Short2ObjectMap.Entry<T> entry = vbucketStates.next();
                short j = entry.getShortKey();
                nextState = entry.getValue();
                if (j == (lastVbid + 1) && Objects.equals(startingState, nextState)) {
                    matchingVbids.add(j);
                    startingVbid = -1;
                    lastVbid = j;
                } else {
                    startingVbid = j;
                    break;
                }
            }
            builder.append('"');
            ShortUtil.appendCompact(matchingVbids.iterator(), builder);
            builder.append(":").append(displayFunction.apply(startingState)).append('"');
            startingState = nextState;
        }
        if (startingVbid != -1) {
            builder.append(",\"").append(startingVbid).append(":").append(displayFunction.apply(nextState)).append('"');
        }
    }
}

/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.state;

import java.util.List;

public class DcpBucketState {
    private static final int pSize = Long.BYTES * 2;
    private final byte[] state;

    public DcpBucketState(byte[] state) {
        this.state = state;
    }

    public DcpBucketState(int numPartitions) {
        this.state = new byte[numPartitions * pSize];
    }

    public DcpBucketState(int numPartitions, byte[][] pss) {
        this(numPartitions);
        for (byte[] ps : pss) {
            update(ps);
        }
    }

    public void update(DcpPartitionState ps) {
        update(ps.serialize());
    }

    public void update(byte[] ps) {
        short partition = DcpPartitionState.vbid(ps);
        int offset = offset(partition);
        setLong(state, offset, DcpPartitionState.uuid(ps));
        setLong(state, offset + Long.BYTES, DcpPartitionState.seq(ps));
    }

    public void merge(DcpBucketState bs) {
    }

    public void merge(byte[] bs) {
    }

    public void min(DcpBucketState bs) {
    }

    public void min(byte[] bs) {
    }

    public boolean within(short vbid, long currentSeq, List<FailoverLogEntry> failoverLog) {
        long uuid = uuid(vbid);
        long seq = seq(vbid);
        int i = failoverLog.size();
        long upperBound = currentSeq;
        while (i > 0) {
            i--;
            FailoverLogEntry entry = failoverLog.get(i);
            if (entry.getUuid() == uuid) {
                return entry.getSeqno() <= seq && seq <= upperBound;
            }
            upperBound = entry.getSeqno();
        }
        return false;
    }

    public long uuid(short vbid) {
        int offset = offset(vbid);
        return getLong(state, offset);
    }

    public long seq(short vbid) {
        int offset = offset(vbid) + Long.BYTES;
        return getLong(state, offset);
    }

    public void uuid(short vbid, long uuid) {
        int offset = offset(vbid);
        setLong(state, offset, uuid);
    }

    public void seq(short vbid, long seq) {
        int offset = offset(vbid) + Long.BYTES;
        setLong(state, offset, seq);
    }

    public void update(short vbid, long uuid, long seq) {
        int offset = offset(vbid);
        setLong(state, offset, uuid);
        setLong(state, offset + Long.BYTES, seq);
    }

    public DcpPartitionState partitionState(short vbid) {
        int offset = offset(vbid);
        long uuid = getLong(state, offset);
        long seq = getLong(state, offset + Long.BYTES);
        return new DcpPartitionState(vbid, uuid, seq);
    }

    private int offset(short partition) {
        return partition * pSize;
    }

    public static long getLong(byte[] bytes, int start) {
        return (((long) (bytes[start] & 0xff)) << 56) + (((long) (bytes[start + 1] & 0xff)) << 48)
                + (((long) (bytes[start + 2] & 0xff)) << 40) + (((long) (bytes[start + 3] & 0xff)) << 32)
                + (((long) (bytes[start + 4] & 0xff)) << 24) + (((long) (bytes[start + 5] & 0xff)) << 16)
                + (((long) (bytes[start + 6] & 0xff)) << 8) + (((long) (bytes[start + 7] & 0xff)) << 0);
    }

    public static void setLong(byte[] bytes, int start, long value) {
        bytes[start] = (byte) ((value >>> 56) & 0xFF);
        bytes[start + 1] = (byte) ((value >>> 48) & 0xFF);
        bytes[start + 2] = (byte) ((value >>> 40) & 0xFF);
        bytes[start + 3] = (byte) ((value >>> 32) & 0xFF);
        bytes[start + 4] = (byte) ((value >>> 24) & 0xFF);
        bytes[start + 5] = (byte) ((value >>> 16) & 0xFF);
        bytes[start + 6] = (byte) ((value >>> 8) & 0xFF);
        bytes[start + 7] = (byte) ((value >>> 0) & 0xFF);
    }

    public static short getShort(byte[] bytes, int start) {
        return (short) (((bytes[start] & 0xff) << 8) + (bytes[start + 1] & 0xff));
    }

    public static void setShort(byte[] bytes, int start, short value) {
        bytes[start] = (byte) ((value >>> 8) & 0xFF);
        bytes[start + 1] = (byte) ((value >>> 0) & 0xFF);
    }
}

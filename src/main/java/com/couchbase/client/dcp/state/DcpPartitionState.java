/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

public class DcpPartitionState {
    /*
     * short: vbucket id
     * long : vbucket uuid
     * long : seq number
     */
    private static final int size = 18;
    private static final int vbidOffset = 0;
    private static final int uuidOffset = Short.BYTES;
    private static final int seqOffset = uuidOffset + Long.BYTES;
    private final byte[] state;

    public DcpPartitionState() {
        state = new byte[size];
    }

    public DcpPartitionState(byte[] state) {
        this.state = state;
    }

    public DcpPartitionState(short vbid, long uuid, long seq) {
        this();
        vbid(vbid);
        uuid(uuid);
        seq(seq);
    }

    public short vbid() {
        return DcpBucketState.getShort(state, vbidOffset);
    }

    public static short vbid(byte[] state) {
        return DcpBucketState.getShort(state, vbidOffset);
    }

    public long uuid() {
        return DcpBucketState.getLong(state, uuidOffset);
    }

    public static long uuid(byte[] state) {
        return DcpBucketState.getLong(state, uuidOffset);
    }

    public long seq() {
        return DcpBucketState.getLong(state, seqOffset);
    }

    public static long seq(byte[] state) {
        return DcpBucketState.getLong(state, seqOffset);
    }

    public void vbid(short vbid) {
        DcpBucketState.setShort(state, vbidOffset, vbid);
    }

    public static void vbid(byte[] state, short vbid) {
        DcpBucketState.setShort(state, vbidOffset, vbid);
    }

    public void uuid(long uuid) {
        DcpBucketState.setLong(state, uuidOffset, uuid);
    }

    public static void uuid(byte[] state, long uuid) {
        DcpBucketState.setLong(state, uuidOffset, uuid);
    }

    public void seq(long seq) {
        DcpBucketState.setLong(state, seqOffset, seq);
    }

    public static void seq(byte[] state, long seq) {
        DcpBucketState.setLong(state, seqOffset, seq);
    }

    public byte[] serialize() {
        return state;
    }

    public static DcpPartitionState deserialize(byte[] state) {
        return new DcpPartitionState(state);
    }

    @Override
    public String toString() {
        return "vbid = " + vbid() + ", uuid = " + uuid() + ", seq = " + seq();
    }
}

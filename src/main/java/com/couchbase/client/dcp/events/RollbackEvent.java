/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import java.io.Serializable;

import com.couchbase.client.dcp.state.PartitionState;

public class RollbackEvent implements PartitionDcpEvent, Serializable {

    private static final long serialVersionUID = 1L;
    private final transient PartitionState ps;
    private final short vbid;
    private long seq;

    public RollbackEvent(PartitionState ps) {
        this.ps = ps;
        vbid = ps.vbid();
    }

    @Override
    public Type getType() {
        return Type.ROLLBACK;
    }

    public short getVbid() {
        return vbid;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    @Override
    public String toString() {
        return "{\"" + DcpEvent.class.getSimpleName() + "\":\"" + getClass().getSimpleName() + "\",\"vbid\":" + vbid
                + ",\"seq\":" + seq + "}";
    }

    @Override
    public PartitionState getPartitionState() {
        return ps;
    }
}

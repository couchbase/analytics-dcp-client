/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

public class RollbackEvent implements DcpEvent {

    private final short vbid;
    private long seq;

    public RollbackEvent(short vbid) {
        this.vbid = vbid;
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

}

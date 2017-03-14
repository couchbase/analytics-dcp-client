package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.conductor.DcpChannel;

public class NotMyVBuvketEvent implements DcpEvent {

    private final DcpChannel channel;
    private final short vbid;

    public NotMyVBuvketEvent(DcpChannel channel, short vbid) {
        this.channel = channel;
        this.vbid = vbid;
    }

    public DcpChannel getChannel() {
        return channel;
    }

    public short getVbid() {
        return vbid;
    }

    @Override
    public Type getType() {
        return Type.NOT_MY_VBUCKET;
    }
}

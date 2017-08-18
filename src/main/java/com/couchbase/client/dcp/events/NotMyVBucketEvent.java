package com.couchbase.client.dcp.events;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;

public class NotMyVBucketEvent implements PartitionDcpEvent {
    private static final Logger LOGGER = Logger.getLogger(NotMyVBucketEvent.class.getName());
    private DcpChannel channel;
    private final PartitionState state;

    public NotMyVBucketEvent(PartitionState state) {
        this.state = state;
    }

    public DcpChannel getChannel() {
        return channel;
    }

    public void setChannel(DcpChannel channel) {
        this.channel = channel;
    }

    public short getVbid() {
        return state.vbid();
    }

    @Override
    public PartitionState getPartitionState() {
        return state;
    }

    @Override
    public Type getType() {
        return Type.NOT_MY_VBUCKET;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(toMap());
        } catch (Exception e) {
            LOGGER.log(Level.WARN, e);
            return "{\"" + this.getClass().getSimpleName() + "\":\"" + e.toString() + "\"}";
        }
    }

    private Map<String, Object> toMap() {
        Map<String, Object> tree = new HashMap<>();
        tree.put(DcpEvent.class.getSimpleName(), this.getClass().getSimpleName());
        tree.put(DcpChannel.class.getSimpleName(), channel.toString());
        tree.put("partition", state.vbid());
        return tree;
    }
}

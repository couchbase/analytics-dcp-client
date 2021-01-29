/*
 * Copyright (c) 2016-2021 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;

public class ChannelDroppedEvent implements DcpEvent {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final DcpChannel channel;
    private final Throwable cause;
    private int fixAttempts = 0;

    public ChannelDroppedEvent(DcpChannel channel, Throwable cause) {
        this.channel = channel;
        this.cause = cause;
    }

    public void incrementAttempts() {
        fixAttempts++;
    }

    public int getAttempts() {
        return fixAttempts;
    }

    @Override
    public Type getType() {
        return Type.CHANNEL_DROPPED;
    }

    public DcpChannel getChannel() {
        return channel;
    }

    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(toMap());
        } catch (Exception e) {
            LOGGER.log(Level.WARN, e);
            return "{\"" + this.getClass().getSimpleName() + "\":\"" + e.toString() + "\"}";
        }
    }

    private Map<String, Object> toMap() {
        Map<String, Object> tree = new HashMap<>();
        tree.put(DcpEvent.class.getSimpleName(), this.getClass().getSimpleName());
        tree.put(DcpChannel.class.getSimpleName(), channel.toString());
        tree.put("cause", cause == null ? "null" : cause.toString());
        return tree;
    }

}

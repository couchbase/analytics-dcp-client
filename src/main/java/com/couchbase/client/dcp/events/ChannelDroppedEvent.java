/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.events;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.dcp.conductor.DcpChannel;

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

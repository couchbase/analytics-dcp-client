/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.message;

/**
 * Code describing why producer decided to close the stream.
 */
public enum StreamEndReason {
    /**
     * Invalid stream end reason
     */
    INVALID(0xFF),
    /**
     * The stream has finished without error.
     */
    OK(0x00),
    /**
     * The close stream command was invoked on this stream causing it to be closed
     * by force.
     */
    CLOSED(0x01),
    /**
     * The state of the VBucket that is being streamed has changed to state that
     * the consumer does not want to receive.
     */
    STATE_CHANGED(0x02),
    /**
     * The stream is closed because the connection was disconnected.
     */
    DISCONNECTED(0x03),
    /**
     * The stream is closing because the client cannot read from the stream fast enough.
     * This is done to prevent the server from running out of resources trying while
     * trying to serve the client. When the client is ready to read from the stream
     * again it should reconnect. This flag is available starting in Couchbase 4.5.
     */
    TOO_SLOW(0x04);

    private final int value;

    StreamEndReason(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    static StreamEndReason of(int value) {
        switch (value) {
            case 0x00:
                return OK;
            case 0x01:
                return CLOSED;
            case 0x02:
                return STATE_CHANGED;
            case 0x03:
                return DISCONNECTED;
            case 0x04:
                return TOO_SLOW;
            default:
                throw new IllegalArgumentException("Unknown stream end reason: " + value);
        }
    }

}

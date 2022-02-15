/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.message;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Code describing why producer decided to close the stream.
 */
public enum StreamEndReason {
    /**
     * Reason used internally to indicate the stream ended because its channel was dropped abruptly
     */
    CHANNEL_DROPPED,
    /**
     * Invalid stream end reason
     */
    UNKNOWN,
    /**
     * The stream has finished without error.
     */
    OK,
    /**
     * The close stream command was invoked on this stream causing it to be closed
     * by force.
     */
    CLOSED,
    /**
     * The state of the VBucket that is being streamed has changed to state that
     * the consumer does not want to receive.
     */
    STATE_CHANGED,
    /**
     * The stream is closed because the connection was disconnected.
     */
    DISCONNECTED,
    /**
     * The stream is closing because the client cannot read from the stream fast enough.
     * This is done to prevent the server from running out of resources trying while
     * trying to serve the client. When the client is ready to read from the stream
     * again it should reconnect. This flag is available starting in Couchbase 4.5.
     */
    TOO_SLOW,
    /**
     * The stream closed early due to backfill failure.
     */
    BACKFILL_FAIL,
    /**
     * The stream closed early because the vbucket is rolling back and
     * downstream needs to reopen the stream and rollback too.
     */
    ROLLBACK,

    /**
     * All filtered collections have been removed so no more data can be sent.
     */
    FILTER_EMPTY,

    /**
     * the stream ended because we lost the necessary privileges to maintain it.
     */
    LOST_PRIVILEGES;

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int ORDINAL_OFFSET = 2;

    static StreamEndReason of(int value) {
        int ordinal = value + ORDINAL_OFFSET;
        if (ordinal >= values().length) {
            LOGGER.warn("Unknown stream end reason: {}", value);
            return UNKNOWN;
        } else {
            return values()[ordinal];
        }
    }

}

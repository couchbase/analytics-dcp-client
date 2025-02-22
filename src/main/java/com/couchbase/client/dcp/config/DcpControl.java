/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.couchbase.client.core.annotation.SinceCouchbase;

/**
 * This class is used during bootstrap to configure all the possible DCP negotiation parameters.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpControl implements Iterable<Map.Entry<String, String>> {

    public static final String OSO_TRUE_WITH_SEQNO_ADVANCED = "true_with_seqno_advanced";
    public static final String MAX_MARKER_VERSION_2_2 = "2.2";

    /**
     * Stores the params to negotiate in a map.
     */
    private Map<String, String> values;

    /**
     * Creates a new {@link DcpControl} instance with no params set upfront.
     */
    public DcpControl() {
        this.values = new HashMap<>();
    }

    /**
     * Store/Override a control parameter.
     *
     * @param name
     *            the name of the control parameter.
     * @param value
     *            the stringified version what it should be set to.
     * @return the {@link DcpControl} instance for chainability.
     */
    public DcpControl put(final Names name, final String value) {
        values.put(name.value(), value);
        return this;
    }

    /**
     * Returns a param if set, otherwise null is returned.
     *
     * @param name
     *            the name of the param.
     * @return the stringified value if set, null otherwise.
     */
    public String get(final Names name) {
        return values.get(name.value());
    }

    /**
     * Shorthand getter to check if buffer acknowledgements are enabled.
     */
    public boolean ackEnabled() {
        String bufSize = values.get(Names.CONNECTION_BUFFER_SIZE.value());
        return bufSize != null && Long.parseLong(bufSize) > 0;
    }

    /**
     * Provides an iterator over the stored values in the map.
     */
    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return values.entrySet().iterator();
    }

    /**
     * All the possible control options available.
     *
     * Note that not all params might be supported by the used server version, see the
     * description for each for more information.
     */
    public enum Names {
        /**
         * Used to enable to tell the Producer that the Consumer supports detecting
         * dead connections through the use of a noop. The value for this message should
         * be set to either "true" or "false". See the page on dead connections for more
         * details. This parameter is available starting in Couchbase 3.0.
         */
        ENABLE_NOOP,
        /**
         * Used to tell the Producer the size of the Consumer side buffer in bytes which
         * the Consumer is using for flow control. The value of this parameter should be
         * an integer in string form between 1 and 2^32. See the page on flow control for
         * more details. This parameter is available starting in Couchbase 3.0.
         */
        CONNECTION_BUFFER_SIZE,
        /**
         * Sets the noop interval on the Producer. Values for this parameter should be
         * an integer in string form between 20 and 10800. This allows the noop interval
         * to be set between 20 seconds and 3 hours. This parameter should always be set
         * when enabling noops to prevent the Consumer and Producer having a different
         * noop interval. This parameter is available starting in Couchbase 3.0.1.
         */
        SET_NOOP_INTERVAL,
        /**
         * Sets the priority that the connection should have when sending data.
         * The priority may be set to "high", "medium", or "low". High priority connections
         * will send messages at a higher rate than medium and low priority connections.
         * This parameter is availale starting in Couchbase 4.0.
         */
        SET_PRIORITY,
        /**
         * Enables sending extended meta data. This meta data is mainly used for internal
         * server processes and will vary between different releases of Couchbase. See
         * the documentation on extended meta data for more information on what will be
         * sent. Each version of Couchbase will support a specific version of extended
         * meta data. This parameter is available starting in Couchbase 4.0.
         */
        ENABLE_EXT_METADATA,
        /**
         * Compresses values using snappy compression before sending them. This parameter
         * is available starting in Couchbase 4.5.
         * @deprecated use {@link #FORCE_VALUE_COMPRESSION} as replacement
         */
        @Deprecated ENABLE_VALUE_COMPRESSION,
        /**
         * Compresses values using snappy compression before sending them. Clients need
         * to negotiate for snappy using HELO as a prerequisite to using this parameter.
         * This parameter is available starting in Couchbase 5.5.
         */
        FORCE_VALUE_COMPRESSION,
        /**
         * Tells the server that the client can tolerate the server dropping the connection.
         * The server will only do this if the client cannot read data from the stream fast
         * enough and it is highly recommended to be used in all situations. We only support
         * disabling cursor dropping for backwards compatibility. This parameter is available
         * starting in Couchbase 4.5.
         */
        SUPPORTS_CURSOR_DROPPING,
        /**
         * Tells the server that the client would like to create multiple DCP streams for a
         * vbucket. Once enabled the client must provide a stream-id value to all
         * stream-requests. **Note** that once enabled on a producer, it cannot be disabled.
         */
        ENABLE_STREAM_ID,
        /**
         * Tells the server that the client supports out of order DCP. The server may, if
         * possible send DCP messages in a different order than sequence number order.
         */
        ENABLE_OUT_OF_ORDER_SNAPSHOTS,
        /**
         * Tells the server what order the client would like to receive backfills in. This
         * option is available only from Couchbase 6.6.
         *
         * Possible values are:
         *         round-robin - vBuckets should be backfilled in round-robin order, reading
         *                       a chunk of data from each in turn (default).
         *         sequential - vBuckets should be backfilled sequentially - all data from
         *                      the first vBucket should be read from disk before advancing to
         *                      the next vBucket.
         */
        BACKFILL_ORDER,
        /**
         * Enables the DCP connection to send up & including the specified version DCP snapshot
         * marker. The v2.2 message format will include the most information from the vbucket, some
         * of which may not be required by all clients. Primarily this marker exists to send the
         * current purge_seqno which can be used to mitigate rollbacks on disconnect see
         * [stream-request-value.md]. The only supported value is <code>2.2</code>.
         */
        @SinceCouchbase("Morpheus") MAX_MARKER_VERSION;

        private final String value;

        Names(String value) {
            this.value = value;
        }

        Names() {
            this.value = name().toLowerCase();
        }

        public String value() {
            return value;
        }
    }

    @Override
    public String toString() {
        return "DcpControl{" + values + '}';
    }
}

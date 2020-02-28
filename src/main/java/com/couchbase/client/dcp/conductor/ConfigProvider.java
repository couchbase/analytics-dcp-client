/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.time.Delay;

public interface ConfigProvider {

    /**
     * Referesh the configurations
     *
     * @throws Throwable
     */
    void refresh() throws Throwable;

    /**
     * Referesh the configurations
     *
     * @param attemptTimeout
     *            The timeout per connection attempt.
     * @param totalTimeout
     *            The total timeout
     * @throws Throwable
     */
    void refresh(long attemptTimeout, long totalTimeout) throws Throwable;

    /**
     * Referesh the configurations
     *
     * @param attemptTimeout
     *            The timeout per connection attempt.
     * @param totalTimeout
     *            The total timeout
     * @param delay
     *            delay between attempts
     * @throws Throwable
     */
    void refresh(long attemptTimeout, long totalTimeout, Delay delay) throws Throwable;

    /**
     * @return the last acquired configuration
     */
    CouchbaseBucketConfig config();

    /**
     * @return true if the kv supports collections
     */
    boolean isCollectionCapable();
}

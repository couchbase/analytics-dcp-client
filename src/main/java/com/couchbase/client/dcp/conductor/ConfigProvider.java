/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.config.CouchbaseBucketConfig;

public interface ConfigProvider {
    /**
     * Referesh the configurations with the provided timeout and number of attempts
     *
     * @param timeout
     *            The timeout per connection. If 0 or negative, the provider will use environment configured timeout
     * @param attempts
     *            The number of attempts to refresh the configurations.
     *            If 0 or negative, the provider will use environment configured max attempts value
     * @param waitBetweenAttempts
     *            The time to wait between attempts
     * @throws Throwable
     */
    void refresh(long timeout, int attempts, long waitBetweenAttempts) throws Throwable;

    /**
     * @return the last acquired configuration
     */
    CouchbaseBucketConfig config();
}

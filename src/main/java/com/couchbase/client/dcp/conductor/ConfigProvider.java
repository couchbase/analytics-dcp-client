/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
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
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.config.CouchbaseBucketConfig;

public interface ConfigProvider {
    void refresh() throws Throwable;

    CouchbaseBucketConfig config();
}

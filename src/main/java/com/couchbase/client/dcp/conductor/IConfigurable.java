/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.config.CouchbaseBucketConfig;

@FunctionalInterface
public interface IConfigurable {
    void configure(CouchbaseBucketConfig config);
}

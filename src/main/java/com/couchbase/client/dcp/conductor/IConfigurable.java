package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.config.CouchbaseBucketConfig;

@FunctionalInterface
public interface IConfigurable {
    void configure(CouchbaseBucketConfig config);
}

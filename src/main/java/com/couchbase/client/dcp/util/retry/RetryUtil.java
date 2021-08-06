/*
 * Copyright 2020-2021 Couchbase, Inc.
 */
package com.couchbase.client.dcp.util.retry;

import org.apache.hyracks.api.util.ExceptionUtils;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.dcp.conductor.MasterDcpChannelNotFoundException;
import com.couchbase.client.dcp.error.BucketNotFoundException;

public class RetryUtil {
    private RetryUtil() {
        throw new AssertionError("do not instantiate");
    }

    public static boolean shouldRetry(Throwable th) {
        Throwable root = ExceptionUtils.getRootCause(th);
        return !((root instanceof BucketNotFoundException) || (root instanceof MasterDcpChannelNotFoundException)
                || (root instanceof CouchbaseException && root.getMessage().contains("Unauthorized"))
                || (root.getMessage() != null && root.getMessage().contains("36: No access")));
    }
}

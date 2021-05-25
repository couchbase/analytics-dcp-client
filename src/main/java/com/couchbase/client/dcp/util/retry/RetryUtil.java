/*
 * Copyright 2020 Couchbase, Inc.
 */
package com.couchbase.client.dcp.util.retry;

import org.apache.hyracks.api.util.ExceptionUtils;

import com.couchbase.client.dcp.conductor.MasterDcpChannelNotFoundException;
import com.couchbase.client.dcp.error.AuthorizationException;
import com.couchbase.client.dcp.error.BucketNotFoundException;
import com.couchbase.client.dcp.util.MemcachedStatus;

public class RetryUtil {
    private RetryUtil() {
        throw new AssertionError("do not instantiate");
    }

    public static boolean shouldRetry(Throwable th) {
        Throwable root = ExceptionUtils.getRootCause(th);
        // TODO(mblow): is BadBucketConfigException ever transient?  if it's not, we can add that to the list...
        return !((root instanceof BucketNotFoundException) || (root instanceof MasterDcpChannelNotFoundException)
                || (root instanceof AuthorizationException)
                || MemcachedStatus.messageContains(root, MemcachedStatus.UNKNOWN_COLLECTION)
                || MemcachedStatus.messageContains(root, MemcachedStatus.NO_ACCESS));
    }
}

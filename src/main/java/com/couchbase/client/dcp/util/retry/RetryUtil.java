/*
 * Copyright 2020-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.util.retry;

import org.apache.hyracks.api.util.ExceptionUtils;

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
        return !((root instanceof BucketNotFoundException) || (root instanceof AuthorizationException)
                || MemcachedStatus.messageContains(root, MemcachedStatus.UNKNOWN_COLLECTION)
                || MemcachedStatus.messageContains(root, MemcachedStatus.NO_ACCESS));
    }
}

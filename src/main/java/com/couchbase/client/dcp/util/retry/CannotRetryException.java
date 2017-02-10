/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.util.retry;

import com.couchbase.client.core.CouchbaseException;

/**
 * A {@link CouchbaseException} that denotes that a retry cycle failed because the maximum allowed attempt
 * count was reached.
 *
 * @see Retry
 * @author Simon Baslé
 * @since 1.0.0
 */
public class CannotRetryException extends CouchbaseException {

    public CannotRetryException(String message) {
        super(message);
    }

    public CannotRetryException(String message, Throwable cause) {
        super(message, cause);
    }
}

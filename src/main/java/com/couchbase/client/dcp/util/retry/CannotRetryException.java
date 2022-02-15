/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.util.retry;

import com.couchbase.client.core.error.CouchbaseException;

/**
 * A {@link CouchbaseException} that denotes that a retry cycle failed because the maximum allowed attempt
 * count was reached.
 *
 * @see Retry
 * @author Simon Baslé
 * @since 1.0.0
 */
public class CannotRetryException extends CouchbaseException {
    private static final long serialVersionUID = 2L;

    public CannotRetryException(String message) {
        super(message);
    }

    public CannotRetryException(String message, Throwable cause) {
        super(message, cause);
    }
}

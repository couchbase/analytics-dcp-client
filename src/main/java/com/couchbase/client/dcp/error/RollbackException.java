/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.error;

import com.couchbase.client.core.CouchbaseException;

/**
 * Indicates a rollback happened, used as an internal messaging event.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class RollbackException extends CouchbaseException {
    private static final long serialVersionUID = -3116060802712271509L;

    public RollbackException() {
    }

    public RollbackException(String message) {
        super(message);
    }

    public RollbackException(String message, Throwable cause) {
        super(message, cause);
    }

    public RollbackException(Throwable cause) {
        super(cause);
    }
}

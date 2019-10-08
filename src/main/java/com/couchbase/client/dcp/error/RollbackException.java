/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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

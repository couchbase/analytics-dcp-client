/*
 * Copyright (c) 2020 Couchbase, Inc.
 */
package com.couchbase.client.dcp.error;

import com.couchbase.client.core.CouchbaseException;

public class AuthorizationException extends CouchbaseException {

    private static final long serialVersionUID = 1L;

    public AuthorizationException(String message) {
        super(message);
    }
}

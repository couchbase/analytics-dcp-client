/*
 * Copyright (c) 2018 Couchbase, Inc.
 */
package com.couchbase.client.dcp.error;

import com.couchbase.client.core.CouchbaseException;

public class BucketNotFoundException extends CouchbaseException {

    private static final long serialVersionUID = 1L;

    public BucketNotFoundException(String message) {
        super(message);
    }
}

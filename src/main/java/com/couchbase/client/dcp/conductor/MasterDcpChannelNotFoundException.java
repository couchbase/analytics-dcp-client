/*
 * Copyright 2020 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.CouchbaseException;

public class MasterDcpChannelNotFoundException extends CouchbaseException {
    private static final long serialVersionUID = 1L;

    public MasterDcpChannelNotFoundException(String message) {
        super(message);
    }
}

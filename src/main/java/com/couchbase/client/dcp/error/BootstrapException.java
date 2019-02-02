/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.error;

import com.couchbase.client.core.CouchbaseException;

/**
 * This exception indicates an error during bootstrap. See the cause and message for more details.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class BootstrapException extends CouchbaseException {
    private static final long serialVersionUID = 4129369535195239711L;

    public BootstrapException() {
    }

    public BootstrapException(String message) {
        super(message);
    }

    public BootstrapException(String message, Throwable cause) {
        super(message, cause);
    }

    public BootstrapException(Throwable cause) {
        super(cause);
    }
}

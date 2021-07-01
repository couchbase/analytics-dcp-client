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

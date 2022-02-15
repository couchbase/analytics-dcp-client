/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.error.CouchbaseException;

/**
 * Created by daschl on 01/09/16.
 */
public class NotMyVbucketException extends CouchbaseException {
    private static final long serialVersionUID = 2L;

    public NotMyVbucketException() {
    }

    public NotMyVbucketException(String message) {
        super(message);
    }

    public NotMyVbucketException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotMyVbucketException(Throwable cause) {
        super(cause);
    }
}

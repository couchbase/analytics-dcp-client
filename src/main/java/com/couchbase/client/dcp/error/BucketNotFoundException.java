/*
 * Copyright 2018-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.error;

import com.couchbase.client.core.CouchbaseException;

public class BucketNotFoundException extends CouchbaseException {

    private static final long serialVersionUID = 1L;

    public BucketNotFoundException(String message) {
        super(message);
    }
}

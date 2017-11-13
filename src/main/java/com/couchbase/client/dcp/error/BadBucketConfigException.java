/*
 * Copyright (c) 2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.error;

import java.io.IOException;

/**
 * This exception indicates receiving unusable configurations from ConfigProvider
 */
public class BadBucketConfigException extends IOException {

    private static final long serialVersionUID = 1L;

    public BadBucketConfigException(String message) {
        super(message);
    }

    public BadBucketConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadBucketConfigException(Throwable cause) {
        super(cause);
    }
}

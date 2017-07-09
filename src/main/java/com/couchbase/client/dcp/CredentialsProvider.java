/*
 * Copyright (c) 2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

import org.apache.commons.lang3.tuple.Pair;

@FunctionalInterface
public interface CredentialsProvider {
    /**
     * Get the username/password pair to use for authentication/authorization
     *
     * @param address
     * @return
     */
    Pair<String, String> get(String address) throws Exception;
}

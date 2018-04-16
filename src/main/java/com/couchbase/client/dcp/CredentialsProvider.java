/*
 * Copyright (c) 2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

import java.net.InetSocketAddress;

import org.apache.commons.lang3.tuple.Pair;

@FunctionalInterface
public interface CredentialsProvider {
    /**
     * Get the username/password pair to use for authentication/authorization
     *
     * @param address
     * @return
     */
    Pair<String, String> get(InetSocketAddress address) throws Exception;
}

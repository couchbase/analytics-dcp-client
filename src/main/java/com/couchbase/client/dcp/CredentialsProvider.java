/*
 * Copyright 2017-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
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

/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp;

import java.util.UUID;

/**
 * The default implementation for the {@link ConnectionNameGenerator}.
 *
 * It generates a new name every time called with a "dcp-java-" prefix followed by a UUID.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DefaultConnectionNameGenerator implements ConnectionNameGenerator {

    public static final ConnectionNameGenerator INSTANCE = new DefaultConnectionNameGenerator();

    private DefaultConnectionNameGenerator() {
    }

    @Override
    public String name() {
        return "dcp-java-" + UUID.randomUUID().toString();
    }
}

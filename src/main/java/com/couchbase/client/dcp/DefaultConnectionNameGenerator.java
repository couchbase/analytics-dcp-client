/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

/**
 * A generic interface which determines the name for a DCP connection.
 *
 * Adhering to the semantics of DCP, it is very important that the names of the connection, if independent
 * also need to be different. Otherwise the server will inevitably close the old connection, leading
 * to weird edge cases. Keep this in mind when implementing the interface or just stick with the
 * default {@link DefaultConnectionNameGenerator}.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public interface ConnectionNameGenerator {

    /**
     * Generate the name for a DCP Connection.
     */
    String name();
}

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
@FunctionalInterface
public interface ConnectionNameGenerator {

    /**
     * Generate the name for a DCP Connection.
     */
    String name();

    /**
     * Returns the supplied generated name in a human-readable fashion.  The results when a non-generated
     * name is supplied to this method is undefined.
     */
    default String displayForm(String name) {
        return name;
    }
}

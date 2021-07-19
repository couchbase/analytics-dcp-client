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

public enum State {
    /**
     * The component is currently disconnected.
     */
    DISCONNECTED,

    /**
     * The component is currently connecting or reconnecting.
     */
    CONNECTING,

    /**
     * The component is connected without degradation.
     */
    CONNECTED,

    /**
     * The component is disconnecting.
     */
    DISCONNECTING
}

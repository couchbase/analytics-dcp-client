/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

public class ImpossibleEvent implements DcpEvent {

    @Override
    public Type getType() {
        return Type.UNEXPECTED_FAILURE;
    }
}

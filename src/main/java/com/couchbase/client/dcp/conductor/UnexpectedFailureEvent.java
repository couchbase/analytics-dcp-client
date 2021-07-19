/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package com.couchbase.client.dcp.conductor;

import java.io.Serializable;

import com.couchbase.client.dcp.events.DcpEvent;

public class UnexpectedFailureEvent implements DcpEvent, Serializable {

    private static final long serialVersionUID = 1L;
    private Throwable cause;

    @Override
    public Type getType() {
        return Type.UNEXPECTED_FAILURE;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public String toString() {
        return "{\"" + DcpEvent.class.getSimpleName() + "\":\"" + getClass().getSimpleName() + "\",\"exception\":\""
                + cause.toString() + "\"}";
    }

}

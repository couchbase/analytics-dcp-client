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

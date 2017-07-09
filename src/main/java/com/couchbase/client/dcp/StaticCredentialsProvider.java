/*
 * Copyright (c) 2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

import java.io.Serializable;

import org.apache.commons.lang3.tuple.Pair;

public class StaticCredentialsProvider implements CredentialsProvider, Serializable {
    private static final long serialVersionUID = 1L;
    private final Pair<String, String> credentials;

    public StaticCredentialsProvider(String username, String password) {
        credentials = Pair.of(username, password);
    }

    public StaticCredentialsProvider(Pair<String, String> credentials) {
        this.credentials = credentials;
    }

    @Override
    public Pair<String, String> get(String address) {
        return credentials;
    }

}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state.json;

import java.io.IOException;

import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonToken;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.DeserializationContext;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonDeserializer;

/**
 * Jackson JSON deserializer for {@link SessionState} and {@link PartitionState}.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class SessionStateDeserializer extends JsonDeserializer<SessionState> {

    @Override
    public SessionState deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonToken current = p.getCurrentToken();

        // we ignore the version for now, since there is only one. go directly to the array of states.
        while (current != JsonToken.START_ARRAY) {
            current = p.nextToken();
        }

        current = p.nextToken();
        int i = 0;
        SessionState ss = new SessionState(1024);
        while (current != null && current != JsonToken.END_ARRAY) {
            PartitionState ps = p.readValueAs(PartitionState.class);
            ss.set(i++, ps);
            current = p.nextToken();
        }
        return ss;
    }
}
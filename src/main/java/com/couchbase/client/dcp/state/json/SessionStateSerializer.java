/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state.json;

import java.io.IOException;

import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonGenerator;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonSerializer;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.SerializerProvider;

import rx.functions.Action1;

/**
 * Jackson JSON serializer for {@link SessionState} and {@link PartitionState}.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class SessionStateSerializer extends JsonSerializer<SessionState> {

    @Override
    public void serialize(SessionState ss, final JsonGenerator gen, final SerializerProvider serializers)
            throws IOException {
        gen.writeStartObject();

        gen.writeFieldName("v");
        gen.writeNumber(SessionState.CURRENT_VERSION);

        gen.writeFieldName("ps");
        gen.writeStartArray();
        ss.foreachPartition(new Action1<PartitionState>() {
            @Override
            public void call(PartitionState partitionState) {
                try {
                    gen.writeObject(partitionState);
                } catch (Exception ex) {
                    throw new RuntimeException("Could not serialize PartitionState to JSON: " + partitionState, ex);
                }
            }
        });
        gen.writeEndArray();

        gen.writeEndObject();
    }

}
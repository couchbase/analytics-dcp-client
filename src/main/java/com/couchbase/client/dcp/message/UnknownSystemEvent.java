/*
 * Copyright 2020 Couchbase, Inc.
 */

package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class UnknownSystemEvent extends DcpSystemEvent {
    private static final long serialVersionUID = 1L;

    public UnknownSystemEvent(short vbucket, long seqno, int version, ByteBuf buffer) {
        super(Type.UNKNOWN, vbucket, seqno, version);
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
        throw new IllegalStateException();
    }

    @Override
    public long getManifestUid() {
        throw new IllegalStateException();
    }

    @Override
    public String toString() {
        return "UnknownSystemEvent{" + ", vbucket=" + getVbucket() + ", seqno=" + getSeqno() + ", version="
                + getVersion() + '}';
    }
}

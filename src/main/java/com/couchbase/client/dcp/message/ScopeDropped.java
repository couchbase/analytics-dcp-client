/*
 * Copyright 2020 Couchbase, Inc.
 */

package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class ScopeDropped extends DcpSystemEvent {
    private final long newManifestId;
    private final int scopeId;

    public ScopeDropped(short vbucket, long seqno, int version, ByteBuf buffer) {
        super(Type.SCOPE_DROPPED, vbucket, seqno, version);

        ByteBuf value = MessageUtil.getContent(buffer);

        newManifestId = value.readLong();
        scopeId = value.readInt();
    }

    @Override
    public long getManifestId() {
        return newManifestId;
    }

    public int getScopeId() {
        return scopeId;
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
        return currentManifest.withoutScope(newManifestId, scopeId);
    }

    @Override
    public String toString() {
        return "ScopeDropped{" + "newManifestId=0x" + Long.toUnsignedString(newManifestId, 16) + ", scopeId=0x"
                + Integer.toUnsignedString(scopeId, 16) + ", vbucket=" + getVbucket() + ", seqno=" + getSeqno()
                + ", version=" + getVersion() + '}';
    }
}

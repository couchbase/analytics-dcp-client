/*
 * Copyright 2020 Couchbase, Inc.
 */

package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class ScopeCreated extends DcpSystemEvent {
    private final long newManifestId;
    private final int scopeId;
    private final String scopeName;

    public ScopeCreated(short vbucket, long seqno, int version, ByteBuf buffer) {
        super(Type.SCOPE_CREATED, vbucket, seqno, version);

        scopeName = MessageUtil.getKeyAsString(buffer, false);
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

    public String getScopeName() {
        return scopeName;
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
        return currentManifest.withScope(newManifestId, scopeId, scopeName);
    }

    @Override
    public String toString() {
        return "ScopeCreated{" + "newManifestId=0x" + Long.toUnsignedString(newManifestId, 16) + ", scopeId=0x"
                + Integer.toUnsignedString(scopeId, 16) + ", scopeName='" + scopeName + '\'' + ", vbucket="
                + getVbucket() + ", seqno=" + getSeqno() + ", version=" + getVersion() + '}';
    }
}

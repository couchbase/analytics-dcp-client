/*
 * Copyright 2020 Couchbase, Inc.
 */

package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class CollectionChanged extends DcpSystemEvent {
    private static final long serialVersionUID = 1L;
    private final long newManifestUid;
    private final int collectionId;
    private final int scopeId;
    private final long maxTtl;

    public CollectionChanged(short vbucket, long seqno, int version, ByteBuf buffer) {
        super(Type.COLLECTION_DROPPED, vbucket, seqno, version);

        ByteBuf value = MessageUtil.getContent(buffer);

        newManifestUid = value.readLong();
        scopeId = value.readInt();
        collectionId = value.readInt();
        maxTtl = value.readUnsignedInt();
    }

    @Override
    public long getManifestUid() {
        return newManifestUid;
    }

    public int getScopeId() {
        return scopeId;
    }

    public int getCollectionId() {
        return collectionId;
    }

    public long getMaxTtl() {
        return maxTtl;
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
        return currentManifest.withoutCollection(newManifestUid, collectionId);
    }

    @Override
    public String toString() {
        return "CollectionChanged{" + "newManifestUid=0x" + Long.toUnsignedString(newManifestUid, 16)
                + ", collectionId=0x" + Integer.toUnsignedString(collectionId, 16) + ", scopeId=0x"
                + Integer.toUnsignedString(scopeId, 16) + ", vbucket=" + getVbucket() + ", seqno=" + getSeqno()
                + ", version=" + getVersion() + ", maxTtl=" + maxTtl + '}';
    }
}

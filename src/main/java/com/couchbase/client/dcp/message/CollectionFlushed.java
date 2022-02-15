/*
 * Copyright 2020-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */

package com.couchbase.client.dcp.message;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.util.CollectionsUtil;

public class CollectionFlushed extends DcpSystemEvent {
    private static final long serialVersionUID = 1L;
    private final long newManifestUid;
    private final int collectionId;
    private final int scopeId;

    public CollectionFlushed(short vbucket, long seqno, int version, ByteBuf buffer) {
        super(Type.COLLECTION_FLUSHED, vbucket, seqno, version);

        ByteBuf value = MessageUtil.getContent(buffer);

        newManifestUid = value.readLong();
        scopeId = value.readInt();
        collectionId = value.readInt();
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

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
        return currentManifest.withManifestId(newManifestUid);
    }

    @Override
    public String toString() {
        return "CollectionFlushed{" + "newManifestUid=0x" + Long.toUnsignedString(newManifestUid, 16)
                + ", collectionId=" + CollectionsUtil.displayCid(collectionId) + ", scopeId=0x"
                + Integer.toUnsignedString(scopeId, 16) + ", vbucket=" + getVbucket() + ", seqno=" + getSeqno()
                + ", version=" + getVersion() + '}';
    }
}

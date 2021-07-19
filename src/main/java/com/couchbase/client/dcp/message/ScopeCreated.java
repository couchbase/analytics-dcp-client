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

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class ScopeCreated extends DcpSystemEvent {
    private static final long serialVersionUID = 1L;
    private final long newManifestUid;
    private final int scopeId;
    private final String scopeName;

    public ScopeCreated(short vbucket, long seqno, int version, ByteBuf buffer) {
        super(Type.SCOPE_CREATED, vbucket, seqno, version);

        scopeName = MessageUtil.getKeyAsString(buffer, false);
        ByteBuf value = MessageUtil.getContent(buffer);

        newManifestUid = value.readLong();
        scopeId = value.readInt();
    }

    @Override
    public long getManifestUid() {
        return newManifestUid;
    }

    public int getScopeId() {
        return scopeId;
    }

    public String getScopeName() {
        return scopeName;
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
        return currentManifest.withScope(newManifestUid, scopeId, scopeName);
    }

    @Override
    public String toString() {
        return "ScopeCreated{" + "newManifestUid=0x" + Long.toUnsignedString(newManifestUid, 16) + ", scopeId=0x"
                + Integer.toUnsignedString(scopeId, 16) + ", scopeName='" + scopeName + '\'' + ", vbucket="
                + getVbucket() + ", seqno=" + getSeqno() + ", version=" + getVersion() + '}';
    }
}

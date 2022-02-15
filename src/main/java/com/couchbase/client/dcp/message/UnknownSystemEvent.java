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

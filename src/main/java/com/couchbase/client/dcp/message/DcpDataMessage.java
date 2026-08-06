/*
 * Copyright 2021-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.message;

import org.apache.hyracks.util.annotations.AiProvenance;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

@SuppressWarnings("squid:S1610")
public abstract class DcpDataMessage {

    protected DcpDataMessage() {
        throw new AssertionError("do not instantiate");
    }

    public static int cid(final ByteBuf buffer) {
        return MessageUtil.getCid(buffer);
    }

    /**
     * @deprecated superseded by {@link MessageUtil#getKeyWithCid}, which yields the collection id alongside the key
     *             rather than making the caller parse the buffer a second time to get it.
     *             <p>
     *             Retained only so that a cbas-core predating the stream/collection decoupling still compiles against
     *             this client. The manifest pins this repository by <em>branch</em>, so removing it here before the
     *             cbas-core side has merged would break every cbas-core build on totoro for the window between the
     *             two- not just for the change which needs it. Delete it once that has landed.
     */
    @Deprecated
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "reinstated verbatim; only the deprecation notice is new")
    public static ByteBuf key(final ByteBuf buffer, boolean isCollectionEnabled) {
        return MessageUtil.getKey(buffer, isCollectionEnabled);
    }

    public static String keyString(final ByteBuf buffer, boolean isCollectionEnabled) {
        return MessageUtil.getKeyAsString(buffer, isCollectionEnabled);
    }

    public static short partition(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static long cas(final ByteBuf buffer) {
        return MessageUtil.getCas(buffer);
    }

    public static long bySeqno(final ByteBuf buffer) {
        return buffer.getLong(MessageUtil.getHeaderSize(buffer));
    }

    public static long revisionSeqno(final ByteBuf buffer) {
        return buffer.getLong(MessageUtil.getHeaderSize(buffer) + 8);
    }
}

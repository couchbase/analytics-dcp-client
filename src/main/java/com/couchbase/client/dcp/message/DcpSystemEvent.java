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

import java.io.Serializable;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public abstract class DcpSystemEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final CouchbaseLogger log = CouchbaseLoggerFactory.getInstance(DcpSystemEvent.class.getName());

    public enum Type {
        COLLECTION_CREATED(0),
        COLLECTION_DROPPED(1),
        COLLECTION_FLUSHED(2),
        SCOPE_CREATED(3),
        SCOPE_DROPPED(4),
        COLLECTION_CHANGED(5),
        UNKNOWN(-1);

        private final int code;

        Type(int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    private final Type type;
    private final short vbucket;
    private final long seqno;
    private final int version;

    protected DcpSystemEvent(Type type, short vbucket, long seqno, int version) {
        this.vbucket = vbucket;
        this.type = type;
        this.seqno = seqno;
        this.version = version;
    }

    public Type getType() {
        return type;
    }

    public short getVbucket() {
        return vbucket;
    }

    public long getSeqno() {
        return seqno;
    }

    public int getVersion() {
        return version;
    }

    public abstract CollectionsManifest apply(CollectionsManifest currentManifest);

    public abstract long getManifestUid();

    public static DcpSystemEvent parse(final ByteBuf buffer) {
        final short vbucket = MessageUtil.getVbucket(buffer);
        final ByteBuf extras = MessageUtil.getExtras(buffer);
        final long seqno = extras.readLong();
        final int typeCode = extras.readInt();
        final int version = extras.readUnsignedByte();

        switch (typeCode) {
            case 0:
                return new CollectionCreated(vbucket, seqno, version, buffer);
            case 1:
                return new CollectionDropped(vbucket, seqno, version, buffer);
            case 2:
                return new CollectionFlushed(vbucket, seqno, version, buffer);
            case 3:
                return new ScopeCreated(vbucket, seqno, version, buffer);
            case 4:
                return new ScopeDropped(vbucket, seqno, version, buffer);
            case 5:
                return new CollectionChanged(vbucket, seqno, version, buffer);
            default:
                log.debug("unrecognized DCP system event type {}", typeCode);
                return new UnknownSystemEvent(vbucket, seqno, version, buffer);
        }

    }

    public static long getSeqno(ByteBuf event) {
        return MessageUtil.getExtras(event).readLong();
    }
}

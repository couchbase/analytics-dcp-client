/*
 * Copyright (c) 2016 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

public class StreamRequest {
    private final short partition;
    private final long startSeqno;
    private final long endSeqno;
    private final long vbucketUuid;
    private final long snapshotStartSeqno;
    private final long snapshotEndSeqno;
    private final long manifestUid;

    public StreamRequest(short partition, long startSeqno, long endSeqno, long vbucketUuid, long snapshotStartSeq,
            long snapshotEndSeq, long manifestUid) {
        this.partition = partition;
        this.startSeqno = startSeqno;
        this.endSeqno = endSeqno;
        this.vbucketUuid = vbucketUuid;
        this.snapshotStartSeqno = snapshotStartSeq;
        this.snapshotEndSeqno = snapshotEndSeq;
        this.manifestUid = manifestUid;
    }

    public long getStartSeqno() {
        return startSeqno;
    }

    public long getEndSeqno() {
        return endSeqno;
    }

    public long getVbucketUuid() {
        return vbucketUuid;
    }

    public long getSnapshotStartSeqno() {
        return snapshotStartSeqno;
    }

    public long getSnapshotEndSeqno() {
        return snapshotEndSeqno;
    }

    public short getPartition() {
        return partition;
    }

    public long getManifestUid() {
        return manifestUid;
    }

    @Override
    public String toString() {
        return "partition = " + partition + " startSeqno = " + startSeqno + " endSeqno = " + endSeqno
                + " vbucketUuid = " + vbucketUuid + " snapshotStartSeqno = " + snapshotStartSeqno
                + " snapshotEndSeqno = " + snapshotEndSeqno + " manifestUid = " + manifestUid;
    }
}

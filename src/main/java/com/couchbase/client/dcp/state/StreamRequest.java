/*
 * Copyright (c) 2016 Couchbase, Inc.
 */
package com.couchbase.client.dcp.state;

/**
 * See https://github.com/couchbaselabs/dcp-documentation/blob/master/documentation/commands/stream-request.md
 *
 * Byte/ 0 | 1 | 2 | 3 |
 * / | | | |
 * |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
 * +---------------+---------------+---------------+---------------+
 * 0| Flags |
 * +---------------+---------------+---------------+---------------+
 * 4| RESERVED |
 * +---------------+---------------+---------------+---------------+
 * 8| Start sequence number |
 * | |
 * +---------------+---------------+---------------+---------------+
 * 16| End sequence number |
 * | |
 * +---------------+---------------+---------------+---------------+
 * 24| VBucket UUID |
 * | |
 * +---------------+---------------+---------------+---------------+
 * 32| Snapshot Start Seqno |
 * | |
 * +---------------+---------------+---------------+---------------+
 * 40| Snapshot End Seqno |
 * | |
 * +---------------+---------------+---------------+---------------+
 * Total 48 bytes
 *
 */
public class StreamRequest {
    private final short partition;
    private final long startSeqno;
    private final long endSeqno;
    private final long vbucketUuid;
    private final long snapshotStartSeqno; // where we stopped last time. For our use case, we always use startSeqno
    private final long snapshotEndSeqno; // where we stopped last time. For our use case, we always use startSeqno

    public StreamRequest(short partition, long startSeqno, long endSeqno, long vbucketUuid) {
        this.partition = partition;
        this.startSeqno = startSeqno;
        this.endSeqno = endSeqno;
        this.vbucketUuid = vbucketUuid;
        this.snapshotStartSeqno = startSeqno;
        this.snapshotEndSeqno = startSeqno;
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

    @Override
    public String toString() {
        return "partition = " + partition + " startSeqno = " + startSeqno + " endSeqno = " + endSeqno
                + " vbucketUuid = " + vbucketUuid + " snapshotStartSeqno = " + snapshotStartSeqno
                + " snapshotEndSeqno = " + snapshotEndSeqno;
    }
}

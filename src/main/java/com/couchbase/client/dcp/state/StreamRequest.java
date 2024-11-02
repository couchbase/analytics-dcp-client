/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.state;

import static com.couchbase.client.dcp.util.CollectionsUtil.displayCids;
import static com.couchbase.client.dcp.util.CollectionsUtil.displayManifestUid;

public class StreamRequest {
    private final short partition;
    private final long startSeqno;
    private final long endSeqno;
    private final long vbucketUuid;
    private final long snapshotStartSeqno;
    private final long snapshotEndSeqno;
    private final long purgeSeqno;
    private final long manifestUid;
    private final int streamId;
    private final int[] cids;

    public StreamRequest(short partition, long startSeqno, long endSeqno, long vbucketUuid, long snapshotStartSeq,
            long snapshotEndSeq, long purgeSeqno, long manifestUid, int streamId, int... cids) {
        this.partition = partition;
        this.startSeqno = startSeqno;
        this.endSeqno = endSeqno;
        this.vbucketUuid = vbucketUuid;
        this.snapshotStartSeqno = snapshotStartSeq;
        this.snapshotEndSeqno = snapshotEndSeq;
        this.purgeSeqno = purgeSeqno;
        this.manifestUid = manifestUid;
        this.streamId = streamId;
        this.cids = cids;
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

    public long getPurgeSeqno() {
        return purgeSeqno;
    }

    public short getPartition() {
        return partition;
    }

    public long getManifestUid() {
        return manifestUid;
    }

    public int getStreamId() {
        return streamId;
    }

    public int[] getCids() {
        return cids;
    }

    @Override
    public String toString() {
        return String.format("sid %d vbid %d manifestUid %s cids %s %s-%s snap %s-%s purge %s", streamId, partition,
                displayManifestUid(manifestUid), displayCids(cids), Long.toUnsignedString(startSeqno),
                Long.toUnsignedString(endSeqno), Long.toUnsignedString(snapshotStartSeqno),
                Long.toUnsignedString(snapshotEndSeqno), Long.toUnsignedString(purgeSeqno));
    }
}

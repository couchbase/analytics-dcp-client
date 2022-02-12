/*
 * Copyright 2021-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.state;

import static com.couchbase.client.dcp.state.StreamPartitionState.INVALID_SEQNO;
import static com.couchbase.client.dcp.util.CollectionsUtil.displayCid;

import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.util.MathUtil;
import com.couchbase.client.dcp.util.ShortSortedBitSet;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import it.unimi.dsi.fastutil.shorts.ShortSortedSet;

public class CollectionState {
    private static final Logger LOGGER = LogManager.getLogger();

    private final int cid;
    private final ShortSortedSet pendingVbids;
    private Throwable failure;
    private final long[] seqnos;
    private long pendingItemCount = 0;
    private long itemCount = -1;
    private final Semaphore itemCountSemaphore = new Semaphore(0);

    public CollectionState(int cid, int numOfPartitions) {
        this.cid = cid;
        pendingVbids = new ShortSortedBitSet(numOfPartitions);
        seqnos = new long[numOfPartitions];
        Arrays.fill(seqnos, INVALID_SEQNO);
    }

    public long getSeqno(short vbid) {
        return seqnos[vbid];
    }

    public void ensureMaxSeqno(short vbid, long seqno) {
        seqnos[vbid] = MathUtil.maxUnsigned(seqno, seqnos[vbid]);
    }

    public synchronized void prepareForRequest() {
        failure = null;
        for (short vbid = 0; vbid < seqnos.length; vbid++) {
            pendingVbids.add(vbid);
        }
    }

    synchronized void waitForSeqnos(Span span) throws Throwable {
        while (failure == null && hasPendingSeqnos() && !span.elapsed()) {
            span.wait(this);
        }
        if (failure != null) {
            throw failure;
        }
        if (hasPendingSeqnos()) {
            throw new TimeoutException(span + " elapsed before obtaining current seqnos (" + pendingVbids.size()
                    + " remaining (" + pendingVbids + ")");
        }
    }

    public synchronized boolean hasPendingSeqnos() {
        return !pendingVbids.isEmpty();
    }

    public synchronized void handleSeqnoResponse(ByteBuf content) {
        int size = content.readableBytes();
        for (int offset = 0; offset < size; offset += 10) {
            short vbid = content.getShort(offset);
            long seq = content.getLong(offset + Short.BYTES);
            seqnos[vbid] = seq;
            pendingVbids.remove(vbid);
        }
        if (pendingVbids.isEmpty()) {
            notifyAll();
        }
    }

    public synchronized void onFailure(Throwable th) {
        failure = th;
        notifyAll();
    }

    public long[] getSeqnos() {
        return seqnos.clone();
    }

    public long getItemCount() {
        return itemCount;
    }

    public synchronized void resetItemCountRequest() {
        if (itemCountSemaphore.drainPermits() != 0) {
            LOGGER.debug("making new (cid {}) item count request before previous finished", displayCid(cid));
            pendingItemCount = 0;
        }
    }

    public synchronized void registerItemResponse() {
        itemCountSemaphore.release();
    }

    public synchronized void recordItemCountResponse(long itemCount) {
        if (!itemCountSemaphore.tryAcquire()) {
            LOGGER.warn("received unexpected collection item count!");
        } else {
            pendingItemCount += itemCount;
            if (itemCountSemaphore.availablePermits() == 0) {
                LOGGER.debug("setting cid {} item count to {} (was {})", displayCid(cid), pendingItemCount,
                        this.itemCount);
                this.itemCount = pendingItemCount;
                pendingItemCount = 0;
            }
        }

    }
}

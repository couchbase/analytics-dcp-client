/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package com.couchbase.client.dcp.conductor;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.hyracks.util.Span;
import org.apache.hyracks.util.annotations.GuardedBy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.error.BucketNotFoundException;
import com.couchbase.client.dcp.events.ChannelDroppedEvent;
import com.couchbase.client.dcp.events.DcpEvent;
import com.couchbase.client.dcp.events.DeadConnectionDetection;
import com.couchbase.client.dcp.events.OpenStreamResponse;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.state.StreamState;
import com.couchbase.client.dcp.util.MemcachedStatus;
import com.couchbase.client.dcp.util.ShortSortedBitSet;
import com.couchbase.client.dcp.util.ShortUtil;
import com.couchbase.client.dcp.util.retry.RetryUtil;

import it.unimi.dsi.fastutil.ints.IntSet;

public class Fixer implements Runnable, SystemEventHandler {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final DcpEvent POISON_PILL = () -> DcpEvent.Type.DISCONNECT;
    private static final Delay DELAY = Delay.fixed(0, TimeUnit.MILLISECONDS);
    private static final long CONFIG_PROVIDER_ATTEMPT_TIMEOUT = 1000;
    private static final long DCP_CHANNEL_ATTEMPT_TIMEOUT = 1000;
    // Total timeout only control re-attempts. 0 -> a single attempt
    private static final long TOTAL_TIMEOUT = 0;
    private static final int MAX_REATTEMPTS_NO_MASTER = 100;
    private static final int MAX_REATTEMPTS_ON_EXCEPTION = 5;
    private final Conductor conductor;
    private final UnexpectedFailureEvent failure = new UnexpectedFailureEvent();
    private Span nextFailed = Span.ELAPSED;
    private volatile boolean running;

    // unbounded
    private final LinkedBlockingQueue<DcpEvent> inbox = new LinkedBlockingQueue<>();
    private final Deque<DcpEvent> backlog = new ArrayDeque<>(1024);

    public Fixer(Conductor conductor) {
        this.conductor = conductor;
        running = false;
    }

    public boolean poison() {
        if (running) {
            backlog.clear();
            inbox.clear();
            inbox.offer(POISON_PILL);
            if (!running) {
                LOGGER.warn("Poisoning the fixer and finding that it was running but it is not running anymore."
                        + " Cleaning the inbox");
                inbox.clear();
            }
        } else {
            LOGGER.info("Poisoning the fixer and finding that it is not running. Do nothing.");
        }
        return true;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(toString());
        try {
            start();
            DeadConnectionDetection detection = new DeadConnectionDetection(conductor);
            DcpEvent next = inbox.take();
            while (next != POISON_PILL) {
                if (next != null) {
                    handle(next);
                } else {
                    attemptFixingBroken();
                    detection.run();
                }
                next = backlog.isEmpty() ? inbox.poll(detection.nanosTilNextCheck(), TimeUnit.NANOSECONDS)
                        : inbox.poll(
                                Long.min(detection.nanosTilNextCheck(), nextFailed.remaining(TimeUnit.NANOSECONDS)),
                                TimeUnit.NANOSECONDS);
            }
            LOGGER.info("{} has been poisoned", this);
        } catch (InterruptedException ie) {
            LOGGER.warn("{} has been interrupted", this);
            Thread.currentThread().interrupt();
        }
        running = false;
        reset();
    }

    private synchronized void start() {
        running = true;
        notifyAll();
    }

    public synchronized void waitTillStarted() throws InterruptedException {
        while (!running) {
            wait();
        }
    }

    private void attemptFixingBroken() {
        if (backlog.isEmpty() || !nextFailed.elapsed()) {
            return;
        }
        nextFailed = Span.ELAPSED;
        // we don't want to process entries we push back onto the backlog; remember the size
        int size = backlog.size();
        for (int i = 0; i < size; i++) {
            DcpEvent failedEvent = backlog.poll();
            if (failedEvent == null) {
                // someone has cleared the backlog (e.g. we were poisoned)
                break;
            }
            if (failedEvent.delay().elapsed()) {
                inbox.add(failedEvent);
            } else {
                addToBacklog(failedEvent);
            }
        }
    }

    private void addToBacklog(DcpEvent failedEvent) {
        Span delay = failedEvent.delay();
        if (backlog.isEmpty() || delay.remaining(TimeUnit.NANOSECONDS) < nextFailed.remaining(TimeUnit.NANOSECONDS)) {
            nextFailed = delay;
        }
        backlog.add(failedEvent);
    }

    private void reset() {
        inbox.clear();
        backlog.clear();
    }

    private void handle(DcpEvent event) throws InterruptedException {
        try {
            switch (event.getType()) {
                case CHANNEL_DROPPED:
                    // Channel was dropped and failed to be re-created. Must fix all partitions
                    // that belonged to that channel:
                    // The fix is to refresh the configs, locate dropped channels are. There are three cases:
                    // 1. the kv node is still there and still owns the vbuckets.
                    //    wait for 200ms, refresh 5 times and if nothing changes, fail.
                    // 2. the partition is now owned by an existing connected kv node channel, add a stream there.
                    // 3. the partition is owned by a new kv node.
                    //    add a new channel and establish the stream for the partition
                    LOGGER.info("Handling {}", event);
                    fixDroppedChannel((ChannelDroppedEvent) event);
                    break;
                case OPEN_STREAM_RESPONSE:
                    OpenStreamResponse response = (OpenStreamResponse) event;
                    if (response.getStatus() == MemcachedStatus.MANIFEST_IS_AHEAD) {
                        if (response.delay().elapsed()) {
                            LOGGER.info("Handling {}", event);
                            try {
                                restartStream(response);
                            } catch (Throwable th) {
                                LOGGER.warn("Failure during attempt to handle {} event",
                                        MemcachedStatus.toString(response.getStatus()), th);
                                addToBacklog(response);
                            }
                        } else {
                            addToBacklog(response);
                        }
                    } else {
                        // Refresh the config, find the assigned kv node
                        // (could still be the same one), and re-attempt connection
                        // this should never cause a permanent failure
                        if (response.getStatus() == MemcachedStatus.INVALID_ARGUMENTS) {
                            throw new IllegalStateException("Open stream was created with illegal arguments: "
                                    + response.getPartitionState().getStreamRequest());
                        } else if (response.delay().elapsed()) {
                            LOGGER.info("Handling {}", event);
                            refreshConfig();
                            try {
                                synchronized (conductor.getChannels()) {
                                    CouchbaseBucketConfig config = conductor.config();
                                    int index = config.nodeIndexForMaster(response.getPartitionState().vbid(), false);
                                    NodeInfo node = config.nodeAtIndex(index);
                                    conductor.add(node, config, DCP_CHANNEL_ATTEMPT_TIMEOUT, TOTAL_TIMEOUT, DELAY);
                                    restartStream(response);
                                }
                            } catch (InterruptedException e) {
                                LOGGER.warn("Interrupted while handling not my vbucket event", e);
                                giveUp(e, false);
                                throw e;
                            } catch (Throwable th) {
                                LOGGER.warn("Failure during attempt to handle not my vbucket event", th);
                                addToBacklog(response);
                            }
                        } else {
                            addToBacklog(response);
                        }
                    }
                    break;
                case STREAM_END:
                    LOGGER.info("Handling {}", event);
                    // A stream end can have many reasons.
                    StreamEndEvent streamEndEvent = (StreamEndEvent) event;
                    fixStreamEnd(streamEndEvent);
                    break;
                default:
                    break;
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable th) {
            // there should be a way to pass non-recoverable failures
            LOGGER.warn("Unexpected error in fixer thread while trying to fix a failure", th);
            giveUp(th, true);
        }
    }

    private void restartStream(OpenStreamResponse response) {
        synchronized (conductor.getChannels()) {
            StreamPartitionState partitionState = response.getPartitionState();
            final SessionState sessionState = conductor.getSessionState();
            final int streamId = partitionState.getStreamRequest().getStreamId();
            StreamState streamState = sessionState.streamState(streamId);
            partitionState.prepareNextStreamRequest(sessionState, streamState);
            conductor.startStreamForPartition(partitionState.getStreamRequest());
        }
    }

    private void fixStreamEnd(StreamEndEvent streamEndEvent) throws Throwable {
        switch (streamEndEvent.reason()) {
            case CLOSED:
                // Normal op, user requested close of stream
                LOGGER.info(this + " stream stopped as per your request");
                break;
            case DISCONNECTED:
                // The server is preparing to disconnect. wait for a channel drop which will come soon
                LOGGER.warn(this + " the channel is going to drop. not sure when this could happen."
                        + "Should wait for the drop event before attempting a fix");
                break;
            case OK:
                // Normal op, reached the end of the requested DCP stream
                LOGGER.info(this + " stream reached the end of your request");
                break;
            case LOST_PRIVILEGES:
                LOGGER.info(this + " we have lost privileges");
                break;
            case FILTER_EMPTY:
                LOGGER.info(this + " filter empty (last collection associated with stream was dropped)");
                break;
            case TOO_SLOW:
                // Log, requesting upgrade to analytics resources and re-open the stream
                LOGGER.warn(this + " need more analytics ingestion nodes. we are slow for the producer node");
                doReconnect(streamEndEvent);
                break;
            case UNKNOWN:
                LOGGER.error("{} Stream ended with invalid indicating a producer error, should re-open the stream",
                        this);
                // update the config and attempt to resume
                doReconnect(streamEndEvent);
                break;
            case STATE_CHANGED:
                LOGGER.info(this + " vbucket state has changed");
                break;
            case BACKFILL_FAIL:
            case CHANNEL_DROPPED:
                // update the config and attempt to resume
                doReconnect(streamEndEvent);
                break;
            default:
                LOGGER.error(this + " unexpected event type " + streamEndEvent);
                // update the config and attempt to resume
                doReconnect(streamEndEvent);
                break;
        }
    }

    private void doReconnect(StreamEndEvent streamEndEvent) throws Throwable {
        boolean success = false;
        while (!success) {
            Span attempt = Span.start(1, TimeUnit.SECONDS);
            refreshConfig();
            CouchbaseBucketConfig config = conductor.config();
            StreamPartitionState state = streamEndEvent.getState();
            short index = config.nodeIndexForMaster(streamEndEvent.partition(), false);
            if (index < 0) {
                LOGGER.info(this + " vbucket " + streamEndEvent.partition() + " has no master at the moment");
                retry(streamEndEvent);
                return;
            }
            NodeInfo node = config.nodeAtIndex(index);
            LOGGER.info(this + " was able to find a (new) master for the vbucket " + node.hostname());
            try {
                conductor.add(node, config, DCP_CHANNEL_ATTEMPT_TIMEOUT, TOTAL_TIMEOUT, DELAY);
            } catch (InterruptedException e) {
                LOGGER.warn(this + " interrupted while adding node " + node.hostname(), e);
                giveUp(e, false);
                throw e;
            } catch (Throwable th) {
                LOGGER.warn(this + " failed to add node " + node.hostname(), th);
                if (shouldRetry(streamEndEvent, th)) {
                    attempt.sleep();
                    continue;
                }
                throw th;
            }
            //request again.
            DcpChannel channel = conductor.getChannel(streamEndEvent.partition());
            if (streamEndEvent.isFailoverLogsRequested()) {
                channel.getFailoverLog(streamEndEvent.partition());
            }
            final SessionState sessionState = conductor.getSessionState();
            final int[] seqRequested = streamEndEvent.getSeqRequested();
            if (seqRequested.length > 0) {
                channel.requestSeqnos(seqRequested);
            }
            streamEndEvent.reset();
            state.prepareNextStreamRequest(sessionState, streamEndEvent.getStreamState());
            conductor.startStreamForPartition(state.getStreamRequest());
            success = true;
        }
    }

    private void refreshConfig() throws InterruptedException {
        LOGGER.info(this + " refreshing configurations");
        try {
            conductor.configProvider().refresh(CONFIG_PROVIDER_ATTEMPT_TIMEOUT, TOTAL_TIMEOUT, DELAY);
        } catch (InterruptedException e) {
            LOGGER.error(this + " interrupted while refreshing configurations", e);
            giveUp(e, false);
            throw e;
        } catch (BucketNotFoundException bde) {
            // abort all, close the channels
            throw bde;
        } catch (Throwable th) {
            LOGGER.error(this + " failed to refresh configurations", th);
        }
        LOGGER.info(this + " configurations refreshed");
    }

    private void retry(ChannelDroppedEvent event, Throwable th) throws InterruptedException {
        LOGGER.warn(this + " failed to fix a dropped dcp connection", th);
        event.incrementAttempts();
        if (event.getAttempts() > MAX_REATTEMPTS_ON_EXCEPTION) {
            LOGGER.warn(this + " failed to fix a dropped dcp connection for the " + event.getAttempts()
                    + "th time. Giving up");
            giveUp(th, false);
        } else {
            LOGGER.warn(this + " retrying for the " + event.getAttempts() + " time");
            addToBacklog(event);
        }
    }

    private boolean shouldRetry(StreamEndEvent streamEndEvent, Throwable th) {
        streamEndEvent.incrementAttempts();
        if (streamEndEvent.getAttempts() > MAX_REATTEMPTS_ON_EXCEPTION || !RetryUtil.shouldRetry(th)) {
            LOGGER.warn(this + " failed to fix a vbucket stream " + streamEndEvent.getAttempts() + " times. Giving up",
                    th);
            return false;
        } else {
            LOGGER.warn(this + " retrying for the " + streamEndEvent.getAttempts() + " time");
            return true;
        }
    }

    private void retry(StreamEndEvent streamEndEvent) throws InterruptedException {
        streamEndEvent.incrementAttempts();
        if (streamEndEvent.getAttempts() > MAX_REATTEMPTS_NO_MASTER) {
            LOGGER.warn(this + " failed to fix a vbucket stream " + streamEndEvent.getAttempts() + " times. Giving up");
            giveUp(new NotConnectedException(), false);
        } else {
            addToBacklog(streamEndEvent);
        }
    }

    private void giveUp(Throwable e, boolean wait) throws InterruptedException {
        conductor.disconnect(wait);
        failure.setCause(e);
        conductor.getEnv().eventBus().publish(failure);
    }

    private void fixDroppedChannel(ChannelDroppedEvent event) throws InterruptedException {
        LOGGER.warn(this + " fixing channel dropped... Requesting new configurations");
        try {
            refreshConfig();
        } catch (BucketNotFoundException bde) {
            throw bde;
        } catch (Throwable th) {
            retry(event, th);
            return;
        }
        LOGGER.info(this + " completed refreshing configurations configurations");
        fixChannel(event.getChannel());
    }

    private void fixChannel(DcpChannel channel) throws InterruptedException {
        CouchbaseBucketConfig config = conductor.configProvider().config();
        int numPartitions = conductor.getSessionState().getNumOfPartitions();
        synchronized (conductor.getChannels()) {
            synchronized (channel) {
                if (channel.getState() == State.CONNECTED) {
                    channel.setState(State.DISCONNECTED);
                    if (config.hasPrimaryPartitionsOnNode(channel.getHostname())) {
                        try {
                            LOGGER.debug(this + " trying to reconnect " + channel);
                            channel.connect(DCP_CHANNEL_ATTEMPT_TIMEOUT, TOTAL_TIMEOUT, DELAY);
                            channel.setChannelDroppedReported(false);
                        } catch (InterruptedException e) {
                            LOGGER.error(this + " interrupted while attempting to connect channel:" + channel, e);
                            giveUp(e, false);
                            throw e;
                        } catch (Throwable th) {
                            queueOpenStreams(channel, numPartitions);
                            conductor.removeChannel(channel);
                            LOGGER.warn(
                                    this + " failed to re-establish a failed dcp connection. Must notify the client",
                                    th);
                        }
                    } else {
                        LOGGER.debug(this + " the dropped channel " + channel + " has no vbuckets");
                        queueOpenStreams(channel, numPartitions);
                        conductor.removeChannel(channel);
                    }
                }
            }
        }
    }

    @GuardedBy("channel")
    private void queueOpenStreams(DcpChannel channel, int numPartitions) {
        boolean infoEnabled = LOGGER.isInfoEnabled();
        ShortSortedBitSet toLog = infoEnabled ? new ShortSortedBitSet(numPartitions) : null;
        IntSet[] openStreams = channel.openStreams();
        for (short vb = 0; vb < numPartitions; vb++) {
            short vbid = vb;
            if (openStreams[vbid] != null) {
                openStreams[vbid].intStream().mapToObj(sid -> conductor.getSessionState().streamState(sid))
                        .forEach(ss -> putPartitionInQueue(channel, ss, vbid));
                if (infoEnabled && !openStreams[vbid].isEmpty()) {
                    toLog.add(vbid);
                }
            }
        }
        if (infoEnabled) {
            LOGGER.info("{} server closed stream on vbuckets {} with reason {}", this,
                    ShortUtil.toCompactString(toLog.iterator()), StreamEndReason.CHANNEL_DROPPED);
        }
    }

    private void putPartitionInQueue(DcpChannel channel, StreamState ss, short vb) {
        StreamPartitionState state = ss.get(vb);
        state.setState(StreamPartitionState.DISCONNECTED);
        StreamEndEvent endEvent = new StreamEndEvent(state, ss, StreamEndReason.CHANNEL_DROPPED);
        endEvent.setFailoverLogsRequested(channel.getFailoverLogRequests()[vb]);
        endEvent.setSeqRequested(IntStream.of(ss.cids()).filter(channel::isMissingSeqnos).toArray());
        conductor.getEnv().eventBus().publish(endEvent);
    }

    @Override
    public void onEvent(DcpEvent event) {
        if (running) {
            if (event.getType() == DcpEvent.Type.OPEN_STREAM_ROLLBACK_RESPONSE) {
                inbox.clear();
            } else if (event.getType() == DcpEvent.Type.OPEN_STREAM_RESPONSE) {
                OpenStreamResponse response = (OpenStreamResponse) event;
                if (response.getStatus() == MemcachedStatus.SUCCESS) {
                    return;
                }
                // TODO(mblow): what should we be doing for non-rollback open stream failures??
            }
            inbox.offer(event); // NOSONAR: This will always succeed as the inbox is unbounded
            if (!running) {
                inbox.clear();
            }
        }
    }

    @Override
    public String toString() {
        return "FixerThread:" + conductor.getEnv().connectionNameGenerator().name();
    }
}

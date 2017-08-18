package com.couchbase.client.dcp.conductor;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.events.ChannelDroppedEvent;
import com.couchbase.client.dcp.events.DcpEvent;
import com.couchbase.client.dcp.events.DeadConnectionDetection;
import com.couchbase.client.dcp.events.NotMyVBucketEvent;
import com.couchbase.client.dcp.events.PartitionDcpEvent;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.state.PartitionState;

public class Fixer implements Runnable, SystemEventHandler {
    private static final Logger LOGGER = Logger.getLogger(Fixer.class.getName());
    private static final DcpEvent POISON_PILL = () -> DcpEvent.Type.DISCONNECT;
    private final Conductor conductor;
    private final UnexpectedFailureEvent failure = new UnexpectedFailureEvent();
    private volatile boolean running;

    // unbounded
    private final LinkedBlockingQueue<DcpEvent> inbox = new LinkedBlockingQueue<>();
    private final LinkedList<DcpEvent> failed = new LinkedList<>();

    public Fixer(Conductor conductor) {
        this.conductor = conductor;
        running = false;
    }

    public boolean poison() {
        if (running) {
            inbox.clear();
            inbox.offer(POISON_PILL);
            if (!running) {
                inbox.clear();
            }
        }
        return true;
    }

    @Override
    public void run() {
        try {
            running = true;
            DeadConnectionDetection detection = new DeadConnectionDetection(conductor);
            DcpEvent next = inbox.take();
            while (next == null || next != POISON_PILL) {
                if (next != null) {
                    handle(next);
                } else {
                    attemptFixingBroken();
                    detection.run();
                }
                next = failed.isEmpty() ? inbox.poll(detection.timeToCheck(), TimeUnit.MILLISECONDS)
                        : inbox.poll(100, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException ie) { // NOSONAR
            LOGGER.log(Level.WARN, "Dcp Fixer thread has been interrupted");
        }
        running = false;
        reset();
    }

    private void attemptFixingBroken() {
        inbox.addAll(failed);
        failed.clear();
    }

    private void reset() {
        inbox.clear();
        failed.clear();
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
                    fixDroppedChannel((ChannelDroppedEvent) event);
                    LOGGER.log(Level.WARN, "Handling " + event);
                    break;
                case NOT_MY_VBUCKET:
                    // Refresh the config, find the new assigned kv node
                    // (could still be the same one), and re-attempt connection
                    // this should never cause a permanent failure
                    LOGGER.log(Level.WARN, "Handling " + event);
                    NotMyVBucketEvent notMyVbucketEvent = (NotMyVBucketEvent) event;
                    refreshConfig();
                    try {
                        synchronized (conductor.getChannels()) {
                            CouchbaseBucketConfig config = conductor.config();
                            int index = config.nodeIndexForMaster(notMyVbucketEvent.getVbid(), false);
                            NodeInfo node = config.nodeAtIndex(index);
                            conductor.add(node, config);
                            PartitionState state = notMyVbucketEvent.getPartitionState();
                            state.prepareNextStreamRequest();
                            conductor.startStreamForPartition(state.getStreamRequest());
                        }
                    } catch (InterruptedException e) {
                        LOGGER.log(Level.WARN, "Interrupted while handling not my vbucket event", e);
                        giveUp(notMyVbucketEvent, e);
                        throw e;
                    } catch (Throwable th) {
                        LOGGER.log(Level.ERROR, "Failure during attempt to handle not my vbucket event", th);
                        failed.add(notMyVbucketEvent);
                    }
                    break;
                case ROLLBACK:
                    LOGGER.log(Level.WARN, "Handling " + event);
                    // abort all, close the channels
                    conductor.disconnect(true);
                    break;
                case STREAM_END:
                    LOGGER.log(Level.WARN, "Handling " + event);
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
            LOGGER.log(Level.WARN, "Unexpected error in fixer thread while trying to fix a failure", th);
            conductor.disconnect(true);
            failure.setCause(th);
            conductor.getEnv().eventBus().publish(failure);
        }
    }

    private void fixStreamEnd(StreamEndEvent streamEndEvent) throws InterruptedException {
        switch (streamEndEvent.reason()) {
            case CLOSED:
                // Normal op, user requested close of stream
                LOGGER.log(Level.INFO, "Stream stopped as per your request");
                break;
            case DISCONNECTED:
                // The server is preparing to disconnect. wait for a channel drop which will come soon
                LOGGER.log(Level.WARN, "The channel is going to drop. not sure when this could happen."
                        + "Should wait for the drop event before attempting a fix");
                break;
            case INVALID:
                LOGGER.log(Level.ERROR, "This should never happen. must abort");
                break;
            case OK:
                // Normal op, reached the end of the requested DCP stream
                LOGGER.log(Level.INFO, "Stream reached the end of your request");
                break;
            case STATE_CHANGED:
            case CHANNEL_DROPPED:
                // Preparing to rebalance, update the config
                // get the new master for the partition and resume from there
                refreshConfig();
                CouchbaseBucketConfig config = conductor.config();
                PartitionState state = streamEndEvent.getState();
                short index = config.nodeIndexForMaster(streamEndEvent.partition(), false);
                if (index >= 0) {
                    NodeInfo node = config.nodeAtIndex(index);
                    LOGGER.log(Level.INFO, "Was able to find a new master for the vbucket " + node.hostname());
                    try {
                        conductor.add(node, config);
                    } catch (InterruptedException e) {
                        LOGGER.log(Level.WARN, "Interrupting while adding node " + node.hostname(), e);
                        giveUp(streamEndEvent, e);
                        throw e;
                    } catch (Throwable th) {
                        LOGGER.log(Level.WARN, "Failed to add node " + node.hostname(), th);
                        retry(streamEndEvent, th);
                        break;
                    }
                    //request again.
                    DcpChannel channel = conductor.getChannel(streamEndEvent.partition());
                    if (streamEndEvent.isFailoverLogsRequested()) {
                        channel.getFailoverLog(streamEndEvent.partition());
                    }
                    if (streamEndEvent.isSeqRequested()) {
                        channel.getSeqnos();
                    }
                    streamEndEvent.reset();
                    state.prepareNextStreamRequest();
                    conductor.startStreamForPartition(state.getStreamRequest());
                } else {
                    LOGGER.log(Level.INFO, "Vbucket " + streamEndEvent.partition() + " has no master at the moment");
                    retry(streamEndEvent);
                }
                break;
            case TOO_SLOW:
                // Log, requesting upgrade to analytics resources and re-open the stream
                LOGGER.log(Level.WARN, "Need more analytics ingestion nodes. we are slow for the producer node");
                break;
            default:
                LOGGER.log(Level.ERROR, "Unexpected event type " + streamEndEvent);
                break;
        }
    }

    private void refreshConfig() throws InterruptedException {
        try {
            conductor.configProvider().refresh();
        } catch (InterruptedException e) {
            LOGGER.log(Level.ERROR, "Interrupted while refreshing configurations", e);
            giveUp(e);
            throw e;
        } catch (Throwable th) {
            LOGGER.log(Level.ERROR, "Failed to refresh configurations", th);
        }
    }

    private void retry(StreamEndEvent streamEndEvent, Throwable th) throws InterruptedException {
        streamEndEvent.incrementAttempts();
        if (streamEndEvent.getAttempts() > 100) {
            LOGGER.log(Level.WARN, "Failed to fix a vbucket stream 100 times. Giving up", th);
            giveUp(streamEndEvent, th);
        } else {
            LOGGER.log(Level.WARN, "Retrying for the " + streamEndEvent.getAttempts() + " time");
            failed.add(streamEndEvent);
        }
    }

    private void retry(StreamEndEvent streamEndEvent) throws InterruptedException {
        streamEndEvent.incrementAttempts();
        if (streamEndEvent.getAttempts() > 100) {
            LOGGER.log(Level.WARN, "Failed to fix a vbucket stream 100 times. Giving up");
            giveUp(streamEndEvent, new NotConnectedException());
        } else {
            failed.add(streamEndEvent);
        }
    }

    private void giveUp(PartitionDcpEvent event, Throwable e) throws InterruptedException {
        event.getPartitionState().fail(e);
        giveUp(e);
    }

    private void giveUp(Throwable e) throws InterruptedException {
        conductor.disconnect(false);
        failure.setCause(e);
        conductor.getEnv().eventBus().publish(failure);
    }

    private void fixDroppedChannel(ChannelDroppedEvent event) throws InterruptedException {
        refreshConfig();
        fixChannel(event.getChannel());
    }

    private void fixChannel(DcpChannel channel) throws InterruptedException {
        CouchbaseBucketConfig config = conductor.configProvider().config();
        int numPartitions = conductor.getSessionState().getNumOfPartitions();
        synchronized (conductor.getChannels()) {
            synchronized (channel) {
                if (channel.getState() == State.CONNECTED) {
                    channel.setState(State.DISCONNECTED);
                    if (config.hasPrimaryPartitionsOnNode(channel.getNetworkAddress())) {
                        try {
                            LOGGER.debug("trying to reconnect");
                            channel.connect();
                            channel.setChannelDroppedReported(false);
                        } catch (InterruptedException e) {
                            LOGGER.log(Level.ERROR, "Interrupted while attempting to connect channel", e);
                            giveUp(e);
                            throw e;
                        } catch (Throwable th) {
                            for (short vb = 0; vb < numPartitions; vb++) {
                                if (channel.streamIsOpen(vb)) {
                                    putPartitionInQueue(channel, vb);
                                }
                            }
                            conductor.removeChannel(channel);
                            LOGGER.warn("Failed to re-establish a failed dcp connection. Must notify the client", th);
                        }
                    } else {
                        LOGGER.debug("dropped channel " + channel + " has no vbuckets");
                        for (short vb = 0; vb < numPartitions; vb++) {
                            if (channel.streamIsOpen(vb)) {
                                putPartitionInQueue(channel, vb);
                            }
                        }
                        conductor.removeChannel(channel);
                    }
                }
            }
        }
    }

    private void putPartitionInQueue(DcpChannel channel, short vb) {
        PartitionState state = conductor.getSessionState().get(vb);
        state.setState(PartitionState.DISCONNECTED);
        StreamEndEvent endEvent = state.getEndEvent();
        endEvent.setReason(StreamEndReason.CHANNEL_DROPPED);
        endEvent.setFailoverLogsRequested(channel.getFailoverLogRequests()[vb]);
        endEvent.setSeqRequested(!channel.isStateFetched());
        LOGGER.info("Server closed Stream on vbid " + vb + " with reason " + StreamEndReason.CHANNEL_DROPPED);
        conductor.getEnv().eventBus().publish(endEvent);
    }

    @Override
    public void onEvent(DcpEvent event) {
        if (running) {
            if (event.getType() == DcpEvent.Type.ROLLBACK) {
                inbox.clear();
            }
            inbox.offer(event); // NOSONAR: This will always succeed as the inbox is unbounded
            if (!running) {
                inbox.clear();
            }
        }
    }
}

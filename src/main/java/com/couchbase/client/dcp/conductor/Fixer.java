package com.couchbase.client.dcp.conductor;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.events.ChannelDroppedEvent;
import com.couchbase.client.dcp.events.DcpEvent;
import com.couchbase.client.dcp.events.DcpEvent.Type;
import com.couchbase.client.dcp.events.NotMyVBucketEvent;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.state.PartitionState;

public class Fixer implements Runnable, SystemEventHandler {
    private static final Logger LOGGER = Logger.getLogger(Fixer.class.getName());
    private static final DcpEvent POISON_PILL = () -> Type.BUG;
    private final Conductor conductor;
    private volatile boolean running;

    // unbounded
    private final LinkedBlockingQueue<DcpEvent> inbox = new LinkedBlockingQueue<>();

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
            DcpEvent next = inbox.take();
            while (next != POISON_PILL) {
                fix(next);
                next = inbox.take();
            }
        } catch (InterruptedException ie) { // NOSONAR
            LOGGER.log(Level.WARN, "Dcp Fixer thread has been interrupted");
        }
        running = false;
        inbox.clear();
    }

    private void fix(DcpEvent event) throws InterruptedException {
        try {
            switch (event.getType()) {
                case BUG:
                    LOGGER.log(Level.ERROR, "This should never happen. must abort");
                    break;
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
                    LOGGER.log(Level.WARN, "Channel dropped. should fix all vbuckets");
                    break;
                case NOT_MY_VBUCKET:
                    // wait 100 ms, refresh the config, find the new assigned kv node
                    // (could still be the same one), and re-attempt connection
                    // this should never cause a permanent failure
                    NotMyVBucketEvent notMyVbucketEvent = (NotMyVBucketEvent) event;
                    conductor.configProvider().refresh();
                    CouchbaseBucketConfig config = conductor.config();
                    int index = config.nodeIndexForMaster(notMyVbucketEvent.getVbid(), false);
                    NodeInfo node = config.nodeAtIndex(index);
                    conductor.add(node, config);
                    PartitionState state = notMyVbucketEvent.getPartitionState();
                    state.prepareNextStreamRequest();
                    conductor.startStreamForPartition(state.getStreamRequest());
                    LOGGER.log(Level.WARN,
                            "Attempted to open a vbucket from wrong master. refresh config and try again");
                    break;
                case ROLLBACK:
                    // abort all, close the channels, and send a message to the cc
                    LOGGER.log(Level.WARN, "Rollback for a vbucket. abort");
                    break;
                case STREAM_END:
                    // A stream end can have many reasons.
                    StreamEndEvent streamEndEvent = (StreamEndEvent) event;
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
                            conductor.configProvider().refresh();
                            config = conductor.config();
                            index = config.nodeIndexForMaster(streamEndEvent.partition(), false);
                            node = config.nodeAtIndex(index);
                            conductor.add(node, config);
                            state = streamEndEvent.getState();
                            state.prepareNextStreamRequest();
                            conductor.startStreamForPartition(state.getStreamRequest());
                            break;
                        case TOO_SLOW:
                            // Log, requesting upgrade to analytics resources and re-open the stream
                            LOGGER.log(Level.WARN,
                                    "Need more analytics ingestion nodes. we are slow for the producer node");
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;

            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable th) {
            // there should be a way to pass non-recoverable failures
            LOGGER.log(Level.ERROR, "Unexpected error in fixer thread while trying to fix a failure ", th);
        }
    }

    private void fixDroppedChannel(ChannelDroppedEvent event) throws InterruptedException {
        try {
            DcpChannel channel = event.getChannel();
            conductor.configProvider().refresh();
            CouchbaseBucketConfig config = conductor.configProvider().config();
            synchronized (channel) {
                channel.setState(State.DISCONNECTED);
                if (config.hasPrimaryPartitionsOnNode(channel.getInetAddress())) {
                    try {
                        LOGGER.debug("trying to reconnect");
                        channel.wait(200);
                        channel.connect();
                    } catch (Throwable th) {
                        LOGGER.warn("Failed to re-establish a failed dcp connection. Must notify the client", th);
                    }
                } else {
                    int numPartitions = config.numberOfPartitions();
                    for (short vb = 0; vb < numPartitions; vb++) {
                        if (channel.streamIsOpen(vb)) {
                            channel.openStreams()[vb] = false;
                            PartitionState state = channel.getSessionState().get(vb);
                            state.setState(PartitionState.DISCONNECTED);
                            StreamEndEvent endEvent = state.getEndEvent();
                            endEvent.setReason(StreamEndReason.CHANNEL_DROPPED);
                            LOGGER.info("Server closed Stream on vbid " + vb + " with reason "
                                    + StreamEndReason.CHANNEL_DROPPED);
                            channel.getEnv().eventBus().publish(endEvent);
                        }
                    }
                    conductor.removeChannel(channel);
                }
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable th) {
            // there should be a way to pass non-recoverable failures
            LOGGER.log(Level.ERROR, "Unexpected error in fixer thread while trying to fix a failure ", th);
        }
    }

    @Override
    public void onEvent(DcpEvent event) {
        if (running) {
            inbox.offer(event); // NOSONAR: This will always succeed as the inbox is unbounded
            if (!running) {
                inbox.clear();
            }
        }
    }
}

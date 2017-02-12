/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.deps.io.netty.util.internal.ConcurrentSet;

public class Conductor {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Conductor.class);

    private final ConfigProvider configProvider;
    private final Set<DcpChannel> channels;
    private final ClientEnvironment env;
    private SessionState sessionState;

    public Conductor(final ClientEnvironment env, ConfigProvider cp) {
        this.env = env;
        configProvider = cp == null ? new HttpStreamingConfigProvider(env) : cp;
        channels = new ConcurrentSet<>();
    }

    public SessionState sessionState() {
        return sessionState;
    }

    public void connect() throws Throwable {
        configProvider.refresh();
        createSession(configProvider.config());
    }

    /**
     * Returns true if all channels and the config provider are in a disconnected state.
     */
    public boolean disconnected() {
        for (DcpChannel channel : channels) {
            if (channel.getState() != State.DISCONNECTED) {
                return false;
            }
        }
        return true;
    }

    public void stop() throws InterruptedException {
        LOGGER.warn("Instructed to shutdown.");
        for (DcpChannel channel : channels) {
            channel.disconnect();
        }
    }

    /**
     * Returns the total number of partitions.
     */
    public int numberOfPartitions() {
        return configProvider.config().numberOfPartitions();
    }

    public void getSeqnos() {
        for (DcpChannel channel : channels) {
            getSeqnosForChannel(channel);
        }
    }

    private void getSeqnosForChannel(final DcpChannel dcpChannel) {
        dcpChannel.getSeqnos();
    }

    public void getFailoverLog(final short partition) throws InterruptedException {
        PartitionState ps = sessionState().get(partition);
        ps.failoverRequest();
        masterChannelByPartition(partition).getFailoverLog(partition);
        ps.waitTillFailoverUpdated();
    }

    public void startStreamForPartition(final short partition, final long vbuuid, final long startSeqno,
            final long endSeqno, final long snapshotStartSeqno, final long snapshotEndSeqno)
            throws InterruptedException {
        DcpChannel channel = masterChannelByPartition(partition);
        channel.openStream(partition, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno);
    }

    public void stopStreamForPartition(final short partition) throws InterruptedException {
        if (streamIsOpen(partition)) {
            PartitionState ps = sessionState.get(partition);
            DcpChannel channel = masterChannelByPartition(partition);
            channel.closeStream(partition);
            ps.wait(PartitionState.DISCONNECTED);
        }
    }

    public boolean streamIsOpen(final short partition) {
        DcpChannel channel = masterChannelByPartition(partition);
        return channel.streamIsOpen(partition);
    }

    /**
     * Returns the dcp channel responsible for a given vbucket id according to the current
     * configuration.
     *
     * Note that this doesn't mean that the partition is enabled there, it just checks the current
     * mapping.
     */
    private DcpChannel masterChannelByPartition(short partition) {
        CouchbaseBucketConfig config = configProvider.config();
        int index = config.nodeIndexForMaster(partition, false);
        NodeInfo node = config.nodeAtIndex(index);
        for (DcpChannel ch : channels) {
            if (ch.hostname().equals(node.hostname())) {
                return ch;
            }
        }

        throw new IllegalStateException("No DcpChannel found for partition " + partition + ". env vbuckets = "
                + Arrays.toString(env.vbuckets()));
    }

    private void createSession(CouchbaseBucketConfig config) {
        if (sessionState == null) {
            sessionState = new SessionState(config.numberOfPartitions());
        }
    }

    private void reconfigure() throws Throwable {
        List<InetAddress> toAdd = new ArrayList<>();
        List<DcpChannel> toRemove = new ArrayList<>();
        for (NodeInfo node : configProvider.config().nodes()) {
            shouldAdd(node, configProvider.config(), toAdd);
        }
        for (DcpChannel chan : channels) {
            shouldRemove(chan, toRemove, configProvider.config());
        }

        for (DcpChannel remove : toRemove) {
            remove(remove);
        }

        for (InetAddress add : toAdd) {
            add(add);
        }
    }

    private void shouldRemove(DcpChannel chan, List<DcpChannel> toRemove, CouchbaseBucketConfig config) {
        boolean found = false;
        for (NodeInfo node : config.nodes()) {
            InetAddress hostname = node.hostname();
            if (hostname.equals(chan.hostname())) {
                found = true;
                break;
            }
        }
        if (!found) {
            LOGGER.debug("Planning to remove {}", chan);
            toRemove.add(chan);
        }
    }

    private void shouldAdd(NodeInfo node, CouchbaseBucketConfig config, List<InetAddress> toAdd) {
        InetAddress hostname = node.hostname();
        if (!(node.services().containsKey(ServiceType.BINARY) || node.sslServices().containsKey(ServiceType.BINARY))) {
            return;
        }
        boolean in = false;
        for (DcpChannel chan : channels) {
            if (chan.hostname().equals(hostname)) {
                in = true;
                break;
            }
        }
        if (!in && config.hasPrimaryPartitionsOnNode(hostname)) {
            toAdd.add(hostname);
            LOGGER.debug("Planning to add {}", hostname);
        }
    }

    private void add(final InetAddress node) throws Throwable {
        if (channels.contains(node)) {
            return;
        }
        LOGGER.warn("Adding DCP Channel against {}", node);
        final DcpChannel channel = new DcpChannel(node, env, this);
        channels.add(channel);
        channel.connect();
    }

    private void remove(final DcpChannel node) throws InterruptedException {
        if (channels.remove(node)) {
            LOGGER.debug("Removing DCP Channel against {}", node);
            node.disconnect();
        }
    }

    public SessionState getSessionState() {
        return sessionState;
    }

    public void setSessionState(SessionState sessionState) {
        this.sessionState = sessionState;
    }

    public CouchbaseBucketConfig config() {
        return configProvider.config();
    }

    public void establishDcpConnections() throws Throwable {
        reconfigure();
    }

    public DcpChannel getChannel(short vbid) {
        return masterChannelByPartition(vbid);
    }
}

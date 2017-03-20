/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamRequest;

public class Conductor {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Conductor.class);

    private final ConfigProvider configProvider; // changes
    private final Map<InetAddress, DcpChannel> channels; // changes
    private final ClientEnvironment env; // constant
    private SessionState sessionState; // final
    private final Fixer fixer; // final
    private Thread fixerThread; // once per connect
    private volatile boolean connected = false;
    private volatile boolean established;

    public Conductor(final ClientEnvironment env, ConfigProvider cp) {
        this.env = env;
        configProvider = cp == null ? new HttpStreamingConfigProvider(env) : cp;
        channels = new ConcurrentHashMap<>();
        fixer = new Fixer(this);
        env.setSystemEventHandler(fixer);
    }

    public ClientEnvironment getEnv() {
        return env;
    }

    public SessionState getSessionState() {
        return sessionState;
    }

    public void connect() throws Throwable {
        if (connected) {
            return;
        }
        connected = true;
        try {
            channels.clear();
            configProvider.refresh();
            createSession(configProvider.config());
        } catch (Exception e) {
            connected = false;
            throw e;
        }
    }

    /**
     * Returns true if all channels and the config provider are in a disconnected state.
     */
    public boolean disconnected() {
        return !connected;
    }

    public void disconnect() throws InterruptedException {
        fixer.poison();
        if (!connected) {
            return;
        }
        if (fixerThread != null && Thread.currentThread() != fixerThread) {
            fixerThread.join();
            fixerThread = null;
        }
        synchronized (this) {
            if (!connected) {
                return;
            }
            connected = false;
            LOGGER.debug("Instructed to shutdown.");
            for (DcpChannel channel : channels.values()) {
                channel.disconnect();
            }
            established = false;
        }
    }

    /**
     * Returns the total number of partitions.
     */
    public int numberOfPartitions() {
        return configProvider.config().numberOfPartitions();
    }

    public void getSeqnos() throws InterruptedException {
        for (DcpChannel channel : channels.values()) {
            getSeqnosForChannel(channel);
        }
    }

    private void getSeqnosForChannel(final DcpChannel dcpChannel) throws InterruptedException {
        dcpChannel.getSeqnos(true);
    }

    public void getFailoverLog(final short partition) throws InterruptedException {
        PartitionState ps = getSessionState().get(partition);
        ps.failoverRequest();
        masterChannelByPartition(partition).getFailoverLog(partition);
        ps.waitTillFailoverUpdated();
    }

    public void startStreamForPartition(StreamRequest request) {
        DcpChannel channel = masterChannelByPartition(request.getPartition());
        channel.openStream(request.getPartition(), request.getVbucketUuid(), request.getStartSeqno(),
                request.getEndSeqno(), request.getSnapshotStartSeqno(), request.getSnapshotEndSeqno());
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
        DcpChannel theChannel = channels.get(node.hostname());
        if (theChannel == null) {
            throw new IllegalStateException("No DcpChannel found for partition " + partition + ". env vbuckets = "
                    + Arrays.toString(env.vbuckets()));
        }
        return theChannel;
    }

    private synchronized void createSession(CouchbaseBucketConfig config) {
        if (sessionState == null) {
            sessionState = new SessionState(config.numberOfPartitions());
        }

    }

    public void add(NodeInfo node, CouchbaseBucketConfig config) throws Throwable {
        synchronized (channels) {
            InetAddress hostname = node.hostname();
            if (!(node.services().containsKey(ServiceType.BINARY)
                    || node.sslServices().containsKey(ServiceType.BINARY))) {
                return;
            }
            if (!config.hasPrimaryPartitionsOnNode(hostname)) {
                return;
            }
            if (channels.containsKey(hostname)) {
                return;
            }
            LOGGER.debug("Adding DCP Channel against {}", node);
            final DcpChannel channel =
                    new DcpChannel(hostname, env, sessionState, configProvider.config().numberOfPartitions());
            channels.put(hostname, channel);
            channel.connect();
            LOGGER.debug("Planning to add {}", hostname);
        }
    }

    public void setSessionState(SessionState sessionState) {
        this.sessionState = sessionState;
    }

    public CouchbaseBucketConfig config() {
        return configProvider.config();
    }

    public ConfigProvider configProvider() {
        return configProvider;
    }

    public void establishDcpConnections() throws Throwable {
        if (established) {
            return;
        }
        established = true;
        // create fixer thread
        CouchbaseBucketConfig config = configProvider.config();
        fixerThread = new Thread(fixer);
        fixerThread.start();
        for (NodeInfo node : config.nodes()) {
            add(node, config);
        }
    }

    public DcpChannel getChannel(short vbid) {
        return masterChannelByPartition(vbid);
    }

    public void removeChannel(DcpChannel channel) {
        synchronized (channels) {
            channels.remove(channel.getInetAddress());
        }
    }
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import static com.couchbase.client.core.env.NetworkResolution.EXTERNAL;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.util.NetworkUtil;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.AlternateAddress;
import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.events.ChannelDroppedEvent;
import com.couchbase.client.dcp.message.CollectionsManifest;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamRequest;
import com.couchbase.client.dcp.state.StreamState;

public class Conductor {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Conductor.class);
    public static final String KEY_BUCKET_UUID = "bucket_uuid=";
    private final ConfigProvider configProvider; // changes
    private final Map<InetSocketAddress, DcpChannel> channels; // changes
    private final ClientEnvironment env; // constant
    private SessionState sessionState;
    private final Fixer fixer; // final
    private Thread fixerThread; // once per connect
    private volatile boolean connected = false;
    private volatile boolean established;

    public Conductor(final ClientEnvironment env, ConfigProvider cp) {
        this.env = env;
        configProvider = cp == null ? new NonStreamingConfigProvider(env) : cp;
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

    public void disconnect(boolean wait) throws InterruptedException {
        LOGGER.info("Conductor.disconnect called.");
        fixer.poison();
        if (sessionState != null && Thread.currentThread() != fixerThread) {
            sessionState.setDisconnected();
        }
        if (!connected) {
            return;
        }
        Thread lastFixerThread = fixerThread;
        if (Thread.currentThread() != lastFixerThread && lastFixerThread != null) {
            LOGGER.info("Waiting for fixer thread to finish.");
            lastFixerThread.join();
            LOGGER.info("Fixer thread finished.");
        }
        fixerThread = null;
        synchronized (this) {
            if (!connected) {
                return;
            }
            connected = false;
            LOGGER.info("Instructed to shutdown dcp channels.");
            synchronized (channels) {
                for (DcpChannel channel : channels.values()) {
                    channel.disconnect(false);
                }
                if (wait) {
                    for (DcpChannel channel : channels.values()) {
                        channel.wait(State.DISCONNECTED);
                        LOGGER.info(channel + " disconnected");
                    }
                }
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

    public void waitForSeqnos(int streamId) throws Throwable {
        sessionState.streamState(streamId).waitTillCurrentSeqUpdated(env.partitionRequestsTimeout());
    }

    public void requestSeqnos(int streamId) {
        short[] vbuckets = env.vbuckets();
        final StreamState streamState = sessionState.streamState(streamId);
        LOGGER.debug("Getting sequence numbers for {} vbuckets for cid 0x{} on stream {}", vbuckets.length,
                Integer.toUnsignedString(streamState.collectionId()), streamId);
        streamState.currentSeqRequest(vbuckets.length);
        synchronized (channels) {
            for (DcpChannel channel : channels.values()) {
                channel.getSeqnos(streamState);
            }
        }
    }

    public void requestFailoverLog(short vbid) {
        sessionState.get(vbid).failoverRequest();
        synchronized (channels) {
            masterChannelByPartition(vbid).getFailoverLog(vbid);
        }
    }

    public void waitForFailoverLog(short vbid) throws Throwable {
        sessionState.waitTillFailoverUpdated(vbid, env.partitionRequestsTimeout());
    }

    public void startStreamForPartition(StreamRequest request) {
        synchronized (channels) {
            DcpChannel channel = masterChannelByPartition(request.getPartition());
            channel.openStream(request.getPartition(), request.getVbucketUuid(), request.getStartSeqno(),
                    request.getEndSeqno(), request.getSnapshotStartSeqno(), request.getSnapshotEndSeqno(),
                    request.getManifestUid(), request.getStreamId(), request.getCids());
        }
    }

    /**
     * Returns the dcp channel responsible for a given vbucket id according to the current
     * configuration.
     *
     * Note that this doesn't mean that the partition is enabled there, it just checks the current
     * mapping.
     */
    private DcpChannel masterChannelByPartition(short partition) {
        synchronized (channels) {
            CouchbaseBucketConfig config = configProvider.config();
            int index = config.nodeIndexForMaster(partition, false);
            if (index < 0) {
                throw new CouchbaseException(
                        "partition " + partition + " does not have a master node; config: " + config);
            }
            DcpChannel theChannel = dcpChannelForNode(partition, config.nodeAtIndex(index));
            if (theChannel == null) {
                throw new MasterDcpChannelNotFoundException(
                        "master DcpChannel not found for partition " + partition + "; config: " + config);
            }
            return theChannel;
        }
    }

    private DcpChannel dcpChannelForNode(short partition, NodeInfo node) {
        if (env.networkResolution().equals(EXTERNAL)) {
            AlternateAddress aa = node.alternateAddresses().get(EXTERNAL.name());
            if (aa == null) {
                LOGGER.debug("partition {} master node {} does not provide an external alternate address", partition,
                        NetworkUtil.toHostPort(node.hostname(), node.services().get(ServiceType.CONFIG)));
                return null;
            }
            Map<ServiceType, Integer> services = env.sslEnabled() ? aa.sslServices() : aa.services();
            if (services.containsKey(ServiceType.BINARY)) {
                int altPort = services.get(ServiceType.BINARY);
                InetSocketAddress altAddress = new InetSocketAddress(aa.hostname(), altPort);
                return channels.get(altAddress);
            } else {
                LOGGER.debug(
                        "partition {} master node {} does not provide the KV service on its external alternate address {}",
                        partition, NetworkUtil.toHostPort(node.hostname(), node.services().get(ServiceType.CONFIG)),
                        aa.hostname());
                return null;
            }
        } else {
            InetSocketAddress address = new InetSocketAddress(node.hostname(),
                    (env.sslEnabled() ? node.sslServices() : node.services()).get(ServiceType.BINARY));
            return channels.get(address);
        }
    }

    private synchronized void createSession(CouchbaseBucketConfig config) {
        if (sessionState == null) {
            sessionState = new SessionState(config.numberOfPartitions(), getUuid(config.uri()));
        } else {
            sessionState.setConnected(getUuid(config.uri()));
        }
    }

    public void add(NodeInfo node, CouchbaseBucketConfig config, long attemptTimeout, long totalTimeout, Delay delay)
            throws Throwable {
        synchronized (channels) {
            if (!config.hasPrimaryPartitionsOnNode(node.hostname())) {
                return;
            }
            InetSocketAddress address;
            if (env.networkResolution().equals(EXTERNAL)) {
                AlternateAddress aa = node.alternateAddresses().get(EXTERNAL.name());
                if (aa == null) {
                    LOGGER.warn("node {} does not provide an external alternate address",
                            NetworkUtil.toHostPort(node.hostname(), node.services().get(ServiceType.CONFIG)));
                    return;
                }
                Map<ServiceType, Integer> services = env.sslEnabled() ? aa.sslServices() : aa.services();
                if (!services.containsKey(ServiceType.BINARY)) {
                    LOGGER.warn("node {} does not provide the KV service on its external alternate address {}",
                            NetworkUtil.toHostPort(node.hostname(), node.services().get(ServiceType.CONFIG)),
                            aa.hostname());
                    return;
                }
                int altPort = services.get(ServiceType.BINARY);
                address = new InetSocketAddress(aa.hostname(), altPort);
            } else {
                final Map<ServiceType, Integer> services = env.sslEnabled() ? node.sslServices() : node.services();
                if (!services.containsKey(ServiceType.BINARY)) {
                    return;
                }
                address = new InetSocketAddress(node.hostname(), services.get(ServiceType.BINARY));
            }
            if (channels.containsKey(address)) {
                return;
            }
            DcpChannel channel = new DcpChannel(address, node.hostname(), env, sessionState,
                    configProvider.config().numberOfPartitions(), configProvider.isCollectionCapable());
            LOGGER.debug("Adding DCP Channel against {}", node);
            channel.connect(attemptTimeout, totalTimeout, delay);
            channels.put(address, channel);
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
        InvokeUtil.doUninterruptibly(fixer::waitTillStarted);
        for (NodeInfo node : config.nodes()) {
            add(node, config, env.dcpChannelAttemptTimeout(), env.dcpChannelTotalTimeout(),
                    env.dcpChannelsReconnectDelay());
        }
    }

    public CollectionsManifest getCollectionsManifest() throws InterruptedException, TimeoutException {
        if (config().capabilities().contains(BucketCapabilities.COLLECTIONS)) {
            synchronized (channels) {
                sessionState.requestCollectionsManifest(channels.values().iterator().next());
            }
            return sessionState.waitForCollectionsManifest(env.partitionRequestsTimeout());
        }
        return sessionState.getCollectionsManifest();
    }

    public DcpChannel getChannel(short vbid) {
        return masterChannelByPartition(vbid);
    }

    public void removeChannel(DcpChannel channel) {
        synchronized (channels) {
            channels.remove(channel.getAddress());
        }
    }

    public Map<InetSocketAddress, DcpChannel> getChannels() {
        return channels;
    }

    public void reviveDeadConnections(long attemptTimeout, long totalTimeout, Delay delay) {
        synchronized (channels) {
            for (DcpChannel channel : channels.values()) {
                synchronized (channel) {
                    if (channel.producerDroppedConnection()) {
                        try {
                            channel.disconnect(true);
                            try {
                                channel.connect(attemptTimeout, totalTimeout, delay);
                            } catch (Throwable e) {
                                // Disconnect succeeded but connect failed
                                LOGGER.log(CouchbaseLogLevel.WARN,
                                        "Dead connection detected, channel was disconnected successfully but connecting failed. Creating a channel dropped event",
                                        e);
                                channel.setState(State.CONNECTED);
                                env.eventBus().publish(new ChannelDroppedEvent(channel, e));
                            }
                        } catch (Exception e) {
                            LOGGER.log(CouchbaseLogLevel.WARN,
                                    "Failure disconnecting a dead dcp channel. ignoring till next round", e);
                        }
                    }
                }
            }
        }
    }

    public static String getUuid(String uri) {
        int start = uri.indexOf(KEY_BUCKET_UUID);
        if (start < 0) {
            throw new IllegalArgumentException("uuid was not found in " + uri);
        }
        start += KEY_BUCKET_UUID.length();
        int end = uri.indexOf('&', start);
        return end > 0 ? uri.substring(start, end) : uri.substring(start);
    }
}

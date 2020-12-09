/*
 * Copyright (c) 2016-2020 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

import static com.couchbase.client.dcp.util.MathUtil.maxUnsigned;

import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.conductor.Conductor;
import com.couchbase.client.dcp.conductor.ConfigProvider;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.events.EventBus;
import com.couchbase.client.dcp.message.CollectionsManifest;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamPartitionState;
import com.couchbase.client.dcp.state.StreamRequest;
import com.couchbase.client.dcp.state.StreamState;
import com.couchbase.client.dcp.util.FlowControlCallback;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortList;

/**
 * This {@link Client} provides the main API to configure and use the DCP client.
 * Just an interface to the outside world
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class Client {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Client.class);

    /**
     * The {@link Conductor} handles channels and streams. It's the orchestrator of everything.
     */
    private final Conductor conductor;

    /**
     * The stateful {@link ClientEnvironment}, used internally for centralized config management.
     */
    private final ClientEnvironment env;

    /**
     * If buffer acknowledgment is enabled.
     */
    private final boolean ackEnabled;

    /**
     * Creates a new {@link Client} instance.
     *
     * @param builder
     *            the client config builder.
     */
    public Client(Builder builder) {
        EventLoopGroup eventLoopGroup =
                builder.eventLoopGroup() == null ? new NioEventLoopGroup() : builder.eventLoopGroup();
        env = ClientEnvironment.builder().setConnectionNameGenerator(builder.connectionNameGenerator())
                .setBucket(builder.bucket()).setCredentialsProvider(builder.credentialsProvider())
                .setDcpControl(builder.dcpControl()).setEventLoopGroup(eventLoopGroup, builder.eventLoopGroup() == null)
                .setBufferAckWatermark(builder.bufferAckWatermark()).setBufferPooling(builder.poolBuffers())
                .setConfigProviderAttemptTimeout(builder.configProviderAttemptTimeout())
                .setConfigProviderReconnectDelay(builder.configProviderReconnectDelay())
                .setConfigProviderTotalTimeout(builder.configProviderTotalTimeout())
                .setDcpChannelAttemptTimeout(builder.dcpChannelAttemptTimeout())
                .setDcpChannelsReconnectDelay(builder.dcpChannelsReconnectDelay())
                .setDcpChannelTotalTimeout(builder.dcpChannelTotalTimeout()).setEventBus(builder.eventBus())
                .setSslEnabled(builder.sslEnabled()).setSslKeystoreFile(builder.sslKeystoreFile())
                .setSslKeystorePassword(builder.sslKeystorePassword()).setSslKeystore(builder.sslKeystore())
                .setBootstrapHttpDirectPort(builder.configPort()).setBootstrapHttpSslPort(builder.sslConfigPort())
                .setVbuckets(builder.vbuckets()).setClusterAt(builder.clusterAt())
                .setFlowControlCallback(builder.flowControlCallback()).setUuid(builder.uuid())
                .setDynamicConfigurationNodes(builder.dynamicConfigurationNodes())
                .setNetworkResolution(builder.networkResolution()).build();

        ackEnabled = env.dcpControl().ackEnabled();
        if (ackEnabled && env.ackWaterMark() == 0) {
            throw new IllegalArgumentException("The bufferAckWatermark needs to be set if bufferAck is enabled.");
        }

        conductor = new Conductor(env, builder.configProvider());
        LOGGER.debug("Environment Configuration Used: {}", env);

    }

    /**
     * Allows to configure the {@link Client} before bootstrap through a {@link Builder}.
     *
     * @return the builder to configure the client.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Requests and waits the current sequence numbers from all partitions.
     *
     * @throws Throwable exception which occurred while awaiting for sequence numbers
     * @param streamId
     */
    public void getSequenceNumbers(int streamId) throws Throwable {
        conductor.requestSeqnos(streamId);
        conductor.waitForSeqnos(streamId);
    }

    /**
     * Requests the current sequence numbers from all partitions for the specified stream
     *
     * @param streamId
     */
    public void requestSequenceNumbers(int streamId) {
        conductor.requestSeqnos(streamId);
    }

    /**
     * Waits for requested current sequence numbers from all partitions for the specified stream to arrive
     *
     * @throws Throwable exception which occurred while awaiting for sequence numbers
     * @param streamId
     */
    public void waitForSequenceNumbers(int streamId) throws Throwable {
        conductor.waitForSeqnos(streamId);
    }

    /**
     * Returns the current {@link SessionState}, useful for persistence and inspection.
     *
     * @return the current session state.
     */
    public SessionState sessionState() {
        return conductor.getSessionState();
    }

    /**
     * Stores a {@link ControlEventHandler} to be called when control events happen.
     *
     * All events (passed as {@link ByteBuf}s) that the callback receives need to be handled
     * and at least released (by using {@link ByteBuf#release()}, otherwise they will leak.
     *
     * The following messages can happen and should be handled depending on the needs of the
     * client:
     *
     * - {@link RollbackMessage}: If during a connect phase the server responds with rollback
     * information, this event is forwarded to the callback. Does not need to be acknowledged.
     *
     * - {@link DcpSnapshotMarkerRequest}: Server transmits data in batches called snapshots
     * before sending anything, it send marker message, which contains start and end sequence
     * numbers of the data in it. Need to be acknowledged.
     *
     * Keep in mind that the callback is executed on the IO thread (netty's thread pool for the
     * event loops) so further synchronization is needed if the data needs to be used on a different
     * thread in a thread safe manner.
     *
     * @param controlEventHandler
     *            the event handler to use.
     */
    public void controlEventHandler(final ControlEventHandler controlEventHandler) {
        env.setControlEventHandler(controlEventHandler);
    }

    /**
     * Stores a {@link SystemEventHandler} to be called when control events happen.
     */
    public void systemEventHandler(final SystemEventHandler systemEventHandler) {
        env.setSystemEventHandler(systemEventHandler);
    }

    /**
     * Stores a {@link DataEventHandler} to be called when data events happen.
     *
     * All events (passed as {@link ByteBuf}s) that the callback receives need to be handled
     * and at least released (by using {@link ByteBuf#release()}, otherwise they will leak.
     *
     * The following messages can happen and should be handled depending on the needs of the
     * client:
     *
     * - {@link DcpMutationMessage}: A mtation has occurred. Needs to be acknowledged.
     * - {@link DcpDeletionMessage}: A deletion has occurred. Needs to be acknowledged.
     * - {@link DcpExpirationMessage}: An expiration has occurred. Note that current server versions
     * (as of 4.5.0) are not emitting this event, but in any case you should at least release it to
     * be forwards compatible. Needs to be acknowledged.
     *
     * Keep in mind that the callback is executed on the IO thread (netty's thread pool for the
     * event loops) so further synchronization is needed if the data needs to be used on a different
     * thread in a thread safe manner.
     *
     * @param dataEventHandler
     *            the event handler to use.
     */
    public void dataEventHandler(final ClientDataEventHandler dataEventHandler) {
        env.setDataEventHandler((ackHandle, event) -> {
            if (DcpMutationMessage.is(event)) {
                short partition = DcpMutationMessage.partition(event);
                StreamPartitionState ps = MessageUtil.streamState(event, sessionState()).get(partition);
                ps.setSeqno(DcpMutationMessage.bySeqno(event));
            } else if (DcpDeletionMessage.is(event)) {
                short partition = DcpDeletionMessage.partition(event);
                StreamPartitionState ps = MessageUtil.streamState(event, sessionState()).get(partition);
                ps.setSeqno(DcpDeletionMessage.bySeqno(event));
            }
            dataEventHandler.onEvent(ackHandle, event);
        });
    }

    /**
     * Initializes the underlying connections (not the streams) and sets up everything as needed.
     *
     * @throws Throwable
     */
    public synchronized void connect() throws Throwable {
        if (!conductor.disconnected()) {
            LOGGER.debug("Ignoring duplicate connect attempt, already connecting/connected.");
            return;
        }
        LOGGER.info("Connecting to seed nodes and bootstrapping bucket {}.", env.bucket());
        conductor.connect();
    }

    private void validateStream() {
        if (env.dataEventHandler() == null) {
            throw new IllegalArgumentException("A DataEventHandler needs to be provided!");
        }
        if (env.controlEventHandler() == null) {
            throw new IllegalArgumentException("A ControlEventHandler needs to be provided!");
        }
    }

    /**
     * Disconnect the {@link Client} and shut down all its owned resources.
     *
     * If custom state is used (like a shared {@link EventLoopGroup}), then they must be closed and managed
     * separately after this disconnect process has finished.
     *
     * @throws InterruptedException
     */
    public synchronized void disconnect() throws InterruptedException {
        LOGGER.info("Disconnecting the client: " + env.connectionNameGenerator().name() + " started");
        conductor.disconnect(true);
        LOGGER.info("Shutting down the environment of the client: " + env.connectionNameGenerator().name());
        env.shutdown();
        LOGGER.info("Disconnecting the client: " + env.connectionNameGenerator().name() + " completed");
    }

    /**
     * Start DCP streams based on the initialized state for the given partition IDs (vbids).
     *
     * If no ids are provided, all initialized partitions will be started.
     *
     *
     * @param streamId
     * @param vbids
     *            the partition ids (0-indexed) to start streaming for.
     * @throws InterruptedException
     */
    public void startStreaming(int streamId, short... vbids) throws Throwable {
        final StreamState streamState = sessionState().streamState(streamId);
        validateStream();
        int numPartitions = numPartitions();

        vbids = partitionsForVbids(numPartitions, vbids);
        ensureInitialized(streamState, vbids);
        LOGGER.debug("Stream {} start against {} partitions: {}", streamId, vbids.length, Arrays.toString(vbids));
        for (short vbid : vbids) {
            StreamRequest request = streamState.get(vbid).getStreamRequest();
            conductor.startStreamForPartition(request);
        }
    }

    private void ensureInitialized(StreamState streamState, short[] vbids) throws Throwable {
        ShortList nonInitialized = new ShortArrayList();
        for (short vbid : vbids) {
            final StreamPartitionState ps = streamState.get(vbid);
            if (ps.getStreamRequest() == null) {
                if (!sessionState().get(vbid).hasFailoverLogs()) {
                    ps.prepareNextStreamRequest(sessionState(), streamState);
                } else {
                    conductor.requestFailoverLog(vbid);
                    nonInitialized.add(vbid);
                }
            }
        }
        if (nonInitialized.isEmpty()) {
            return;
        }
        for (short vbid : nonInitialized) {
            conductor.waitForFailoverLog(vbid);
            streamState.get(vbid).prepareNextStreamRequest(sessionState(), streamState);
        }
    }

    public CollectionsManifest getCollectionsManifest() throws Throwable {
        if (!config().capabilities().contains(BucketCapabilities.COLLECTIONS)) {
            return CollectionsManifest.DEFAULT;
        }
        return conductor.getCollectionsManifest();
    }

    /**
     * Helper method to turn the array of vbids into a list.
     *
     * @param numPartitions
     *            the number of partitions on the cluster as a fallback.
     * @param vbids
     *            the potentially empty array of selected vbids.
     * @return a sorted list of partitions to use.
     */
    private short[] partitionsForVbids(int numPartitions, short... vbids) {
        if (vbids.length > 0) {
            Arrays.sort(vbids);
        } else {
            vbids = new short[numPartitions];
            for (short i = 0; i < numPartitions; i++) {
                vbids[i] = i;
            }
        }
        return vbids;
    }

    /**
     * Helper method to return the failover logs for the given partitions (vbids).
     *
     * If the list is empty, the failover logs for all partitions will be returned. Note that the returned
     * ByteBufs can be analyzed using the {@link DcpFailoverLogResponse} flyweight.
     *
     * @param vbids
     *            the partitions to return the failover logs from.
     * @throws Throwable
     */
    public void failoverLogs(short... vbids) throws Throwable {
        vbids = partitionsForVbids(numPartitions(), vbids);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Asking for failover logs on partitions {}", Arrays.toString(vbids));
        }
        for (short vbid : vbids) {
            conductor.requestFailoverLog(vbid);
        }
        LOGGER.debug("Waiting to receive failover logs");
        for (short vbid : vbids) {
            conductor.waitForFailoverLog(vbid);
        }
        LOGGER.debug("Received failover logs");
    }

    public void getFailoverLogs() throws Throwable {
        failoverLogs(env.vbuckets());
    }

    /**
     * Returns the number of partitions on the remote cluster.
     *
     * Note that you must be connected, since the information is loaded form the server configuration.
     * On all OS'es other than OSX it will be 1024, on OSX it is 64. Treat this as an opaque value anyways.
     *
     * @return the number of partitions (vbuckets).
     */
    public int numPartitions() {
        return conductor.numberOfPartitions();
    }

    public CouchbaseBucketConfig config() {
        return conductor.config();
    }

    public boolean isCollectionCapable() {
        return config().capabilities().contains(BucketCapabilities.COLLECTIONS);
    }

    public synchronized void establishDcpConnections() throws Throwable {
        if (env.vbuckets() == null) {
            CouchbaseBucketConfig configs = conductor.config();
            if (configs == null) {
                throw new IllegalArgumentException("Not connected");
            }
            env.vbuckets(range((short) 0, (short) configs.numberOfPartitions()));
        }
        conductor.establishDcpConnections();
    }

    public static short[] range(short from, short length) {
        short[] shorts = new short[length];
        for (short i = 0; i < length; i++) {
            shorts[i] = (short) (from + i);
        }
        return shorts;
    }

    public DcpChannel getChannel(short vbid) {
        return conductor.getChannel(vbid);
    }

    public short[] vbuckets() {
        return env.vbuckets();
    }

    public ClientEnvironment getEnvironment() {
        return env;
    }

    public StreamPartitionState getState(int streamId, short vbid) {
        SessionState ss = conductor.getSessionState();
        return (ss == null) ? null : ss.streamState(streamId).get(vbid);
    }

    public boolean isConnected() {
        return !conductor.disconnected();
    }

    /**
     * Builder object to customize the {@link Client} creation.
     */
    public static class Builder {
        private List<InetSocketAddress> clusterAt =
                Collections.singletonList(InetSocketAddress.createUnresolved("127.0.0.1", 0));;
        private CredentialsProvider credentialsProvider;
        private String connectionString;
        private EventLoopGroup eventLoopGroup;
        private String bucket = "default";
        private String uuid = "";
        private boolean dynamicConfigurationNodes = true;
        private ConnectionNameGenerator connectionNameGenerator = DefaultConnectionNameGenerator.INSTANCE;
        private DcpControl dcpControl = new DcpControl();
        private ConfigProvider configProvider = null;
        private int bufferAckWatermark;
        private boolean poolBuffers = true;
        private EventBus eventBus;
        private boolean sslEnabled = ClientEnvironment.DEFAULT_SSL_ENABLED;
        private String sslKeystoreFile;
        private String sslKeystorePassword;
        private KeyStore sslKeystore;
        private int configPort = ClientEnvironment.BOOTSTRAP_HTTP_DIRECT_PORT;
        private int sslConfigPort = ClientEnvironment.BOOTSTRAP_HTTP_SSL_PORT;
        private short[] vbuckets;
        private FlowControlCallback flowControlCallback = FlowControlCallback.NOOP;
        private NetworkResolution networkResolution = NetworkResolution.DEFAULT;
        // Total timeouts, attempt timeouts, and delays
        private long configProviderAttemptTimeout = ClientEnvironment.DEFAULT_CONFIG_PROVIDER_ATTEMPT_TIMEOUT;
        private long configProviderTotalTimeout = ClientEnvironment.DEFAULT_CONFIG_PROVIDER_TOTAL_TIMEOUT;
        private Delay configProviderReconnectDelay = ClientEnvironment.DEFAULT_CONFIG_PROVIDER_RECONNECT_DELAY;
        private long dcpChannelAttemptTimeout = ClientEnvironment.DEFAULT_DCP_CHANNEL_ATTEMPT_TIMEOUT;
        private long dcpChannelTotalTimeout = ClientEnvironment.DEFAULT_DCP_CHANNEL_TOTAL_TIMEOUT;
        private Delay dcpChannelsReconnectDelay = ClientEnvironment.DEFAULT_DCP_CHANNELS_RECONNECT_DELAY;
        private IntList cids = IntLists.EMPTY_LIST;

        /**
         * The buffer acknowledge watermark in percent.
         *
         * @param watermark
         *            between 0 and 100, needs to be > 0 if flow control is enabled.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder bufferAckWatermark(int watermark) {
            if (watermark > 100 || watermark < 0) {
                throw new IllegalArgumentException(
                        "The bufferAckWatermark is percents, so it needs to be between" + " 0 and 100");
            }
            this.bufferAckWatermark = watermark;
            return this;
        }

        /**
         * The clusterAt to bootstrap against.
         *
         * @param clusterAt
         *            seed nodes.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder clusterAt(final List<InetSocketAddress> clusterAt) {
            this.clusterAt = new ArrayList<>(clusterAt);
            return this;
        }

        /**
         * The clusterAt to bootstrap against.
         *
         * @param clusterAt
         *            seed nodes.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder clusterAt(InetSocketAddress... clusterAt) {
            return clusterAt(Arrays.asList(clusterAt));
        }

        /**
         * The uuid of the bucket
         *
         * @param uuid
         *            the bucket uuid
         * @return this {@link Builder} for nice chainability.
         */
        public Builder uuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public String uuid() {
            return this.uuid;
        }

        /**
         * Whether the addresses of configuration providers should be dynamic
         *
         * @param dynamicConfigurationNodes
         * @return this {@link Builder} for nice chainability.
         */
        public Builder dynamicConfigurationNodes(boolean dynamicConfigurationNodes) {
            this.dynamicConfigurationNodes = dynamicConfigurationNodes;
            return this;
        }

        public boolean dynamicConfigurationNodes() {
            return this.dynamicConfigurationNodes;
        }

        /**
         * Sets a custom event loop group, this is needed if more than one client is initialized and
         * runs at the same time to keep the IO threads efficient and in bounds.
         *
         * @param eventLoopGroup
         *            the group that should be used.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder eventLoopGroup(final EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
            return this;
        }

        public EventLoopGroup eventLoopGroup() {
            return eventLoopGroup;
        }

        /**
         * The name of the bucket to use.
         *
         * @param bucket
         *            name of the bucket
         * @return this {@link Builder} for nice chainability.
         */
        public Builder bucket(final String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder flowControlCallback(final FlowControlCallback callback) {
            this.flowControlCallback = callback;
            return this;
        }

        public FlowControlCallback flowControlCallback() {
            return flowControlCallback;
        }

        /**
         * The credentials provider of the bucket to use.
         *
         * @param credentialsProvider
         *            the credentials provider.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder credentialsProvider(final CredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        /**
         * If specific names for DCP connections should be generated, a custom one can be provided.
         *
         * @param connectionNameGenerator
         *            custom generator.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder connectionNameGenerator(final ConnectionNameGenerator connectionNameGenerator) {
            this.connectionNameGenerator = connectionNameGenerator;
            return this;
        }

        /**
         * Set all kinds of DCP control params - check their description for more information.
         *
         * @param name
         *            the name of the param
         * @param value
         *            the value of the param
         * @return this {@link Builder} for nice chainability.
         */
        public Builder controlParam(final DcpControl.Names name, Object value) {
            this.dcpControl.put(name, value.toString());
            return this;
        }

        /**
         * A custom configuration provider can be shared and passed in across clients. use with care!
         *
         * @param configProvider
         *            the custom config provider.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder configProvider(final ConfigProvider configProvider) {
            this.configProvider = configProvider;
            return this;
        }

        /**
         * If buffer pooling should be enabled (yes by default).
         *
         * @param pool
         *            enable or disable buffer pooling.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder poolBuffers(final boolean pool) {
            this.poolBuffers = pool;
            return this;
        }

        /**
         * Sets a custom DCP channel attempt timeout
         *
         * @param dcpChannelAttemptTimeout
         *            the dcp channel socket connect timeout in milliseconds.
         */
        public Builder dcpChannelAttemptTimeout(long dcpChannelAttemptTimeout) {
            this.dcpChannelAttemptTimeout = dcpChannelAttemptTimeout;
            return this;
        }

        /**
         * Sets a custom DCP channel total attempts timeout
         *
         * @param dcpChannelTotalTimeout
         *            the timeout for the total dcp channel socket connect attempts in milliseconds.
         */
        public Builder dcpChannelTotalTimeout(long dcpChannelTotalTimeout) {
            this.dcpChannelTotalTimeout = dcpChannelTotalTimeout;
            return this;
        }

        /**
         * Time to wait for first configuration during a fetch attempt
         *
         * @param configProviderAttemptTimeout
         *            time in milliseconds.
         */
        public Builder configProviderAttemptTimeout(long configProviderAttemptTimeout) {
            this.configProviderAttemptTimeout = configProviderAttemptTimeout;
            return this;
        }

        /**
         * Time to wait for total configuration fetch attempts
         *
         * @param configProviderTotalTimeout
         *            time in milliseconds.
         */
        public Builder configProviderTotalTimeout(long configProviderTotalTimeout) {
            this.configProviderTotalTimeout = configProviderTotalTimeout;
            return this;
        }

        /**
         * Delay between retry attempts for configuration provider
         *
         * @param configProviderReconnectDelay
         */
        public Builder configProviderReconnectDelay(Delay configProviderReconnectDelay) {
            this.configProviderReconnectDelay = configProviderReconnectDelay;
            return this;
        }

        /**
         * Delay between retry attempts for DCP channels
         *
         * @param dcpChannelsReconnectDelay
         */
        public Builder dcpChannelsReconnectDelay(Delay dcpChannelsReconnectDelay) {
            this.dcpChannelsReconnectDelay = dcpChannelsReconnectDelay;
            return this;
        }

        /**
         * Sets the event bus to an alternative implementation.
         *
         * This setting should only be tweaked in advanced cases.
         */
        public Builder eventBus(final EventBus eventBus) {
            this.eventBus = eventBus;
            return this;
        }

        /**
         * Set if SSL should be enabled (default value {@value ClientEnvironment#DEFAULT_SSL_ENABLED}).
         * If true, also set {@link #sslKeystoreFile(String)} and {@link #sslKeystorePassword(String)}.
         */
        public Builder sslEnabled(final boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return this;
        }

        /**
         * Defines the location of the SSL Keystore file (default value null, none).
         *
         * You can either specify a file or the keystore directly via {@link #sslKeystore(KeyStore)}. If the explicit
         * keystore is used it takes precedence over the file approach.
         */
        public Builder sslKeystoreFile(final String sslKeystoreFile) {
            this.sslKeystoreFile = sslKeystoreFile;
            return this;
        }

        /**
         * Sets the SSL Keystore password to be used with the Keystore file (default value null, none).
         *
         * @see #sslKeystoreFile(String)
         */
        public Builder sslKeystorePassword(final String sslKeystorePassword) {
            this.sslKeystorePassword = sslKeystorePassword;
            return this;
        }

        /**
         * Sets the SSL Keystore directly and not indirectly via filepath.
         *
         * You can either specify a file or the keystore directly via {@link #sslKeystore(KeyStore)}. If the explicit
         * keystore is used it takes precedence over the file approach.
         *
         * @param sslKeystore
         *            the keystore to use.
         */
        public Builder sslKeystore(final KeyStore sslKeystore) {
            this.sslKeystore = sslKeystore;
            return this;
        }

        /**
         * Sets the Port that will be used to get bucket configurations.
         *
         * @param configPort
         *            the port to use
         */
        public Builder configPort(final int configPort) {
            this.configPort = configPort;
            return this;
        }

        /**
         * Sets the Port that will be used to get bucket configurations for encrypted connections
         *
         * @param sslConfigPort
         *            the port to use
         */
        public Builder sslConfigPort(final int sslConfigPort) {
            this.sslConfigPort = sslConfigPort;
            return this;
        }

        /**
         * Create the client instance ready to use.
         *
         * @return the built client instance.
         */
        public Client build() {
            return new Client(this);
        }

        public List<InetSocketAddress> clusterAt() {
            return clusterAt;
        }

        public Builder connectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public void networkResolution(NetworkResolution external) {
            this.networkResolution = external;
        }

        public ConnectionNameGenerator connectionNameGenerator() {
            return connectionNameGenerator;
        }

        public String bucket() {
            return bucket;
        }

        public IntList cids() {
            return this.cids;
        }

        public CredentialsProvider credentialsProvider() {
            return credentialsProvider;
        }

        public DcpControl dcpControl() {
            return dcpControl;
        }

        public int bufferAckWatermark() {
            return bufferAckWatermark;
        }

        public boolean poolBuffers() {
            return poolBuffers;
        }

        public long configProviderAttemptTimeout() {
            return configProviderAttemptTimeout;
        }

        public long configProviderTotalTimeout() {
            return configProviderTotalTimeout;
        }

        public Delay configProviderReconnectDelay() {
            return configProviderReconnectDelay;
        }

        public Delay dcpChannelsReconnectDelay() {
            return dcpChannelsReconnectDelay;
        }

        public long dcpChannelAttemptTimeout() {
            return dcpChannelAttemptTimeout;
        }

        public long dcpChannelTotalTimeout() {
            return dcpChannelTotalTimeout;
        }

        public EventBus eventBus() {
            return eventBus;
        }

        public boolean sslEnabled() {
            return sslEnabled;
        }

        public String sslKeystoreFile() {
            return sslKeystoreFile;
        }

        public String sslKeystorePassword() {
            return sslKeystorePassword;
        }

        public KeyStore sslKeystore() {
            return sslKeystore;
        }

        public int configPort() {
            return configPort;
        }

        public int sslConfigPort() {
            return sslConfigPort;
        }

        public ConfigProvider configProvider() {
            return configProvider;
        }

        public Builder vbuckets(final short[] vbuckets) {
            this.vbuckets = vbuckets;
            return this;
        }

        public short[] vbuckets() {
            return vbuckets;
        }

        public String connectionString() {
            return connectionString;
        }

        public NetworkResolution networkResolution() {
            return networkResolution;
        }
    }

    /**
     * Returns a map of stream ids to array of streamed sequence numbers indexed by vbucket
     */
    public Int2ObjectMap<long[]> getStreamedSequenceNumbers() {
        CouchbaseBucketConfig lastConfig = conductor.config();
        final SessionState sessionState = sessionState();
        if (lastConfig == null || sessionState == null) {
            return Int2ObjectMaps.emptyMap();
        }
        Int2ObjectMap<long[]> result = new Int2ObjectOpenHashMap<>();
        short[] vbuckets = vbuckets();
        sessionState.streamStream().forEach(stream -> {
            long[] currentSequences = new long[numPartitions()];
            for (short next : vbuckets) {
                StreamPartitionState ps = stream.get(next);
                currentSequences[next] = ps == null ? 0 : maxUnsigned(0L, ps.getSeqno());
            }
            result.put(stream.streamId(), currentSequences);
        });
        return result;
    }
}

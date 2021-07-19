/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.config;

import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.env.ConfigParserEnvironment;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.node.DefaultMemcachedHashingStrategy;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.ConnectionNameGenerator;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.CredentialsProvider;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.DefaultConnectionNameGenerator;
import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.config.DcpControl.Names;
import com.couchbase.client.dcp.events.DefaultEventBus;
import com.couchbase.client.dcp.events.EventBus;
import com.couchbase.client.dcp.util.FlowControlCallback;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;

import rx.Completable;
import rx.Observable;

/**
 * The {@link ClientEnvironment} is responsible to carry various configuration and
 * state information throughout the lifecycle.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class ClientEnvironment implements SecureEnvironment, ConfigParserEnvironment {
    /*
     * Config provider connection
     */
    public static final long DEFAULT_CONFIG_PROVIDER_ATTEMPT_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
    public static final long DEFAULT_CONFIG_PROVIDER_TOTAL_TIMEOUT = Long.MAX_VALUE;
    public static final Delay DEFAULT_CONFIG_PROVIDER_RECONNECT_DELAY = Delay.linear(TimeUnit.SECONDS, 5, 1);
    /*
     * DCP connection
     */
    public static final long DEFAULT_DCP_CHANNEL_ATTEMPT_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
    public static final long DEFAULT_DCP_CHANNEL_TOTAL_TIMEOUT = Long.MAX_VALUE;
    public static final Delay DEFAULT_DCP_CHANNELS_RECONNECT_DELAY = Delay.fixed(200, TimeUnit.MILLISECONDS);
    /*
     * Other defaults
     */
    public static final boolean DEFAULT_SSL_ENABLED = false;
    public static final int BOOTSTRAP_HTTP_DIRECT_PORT = 8091;
    public static final int BOOTSTRAP_HTTP_SSL_PORT = 18091;
    public static final long DEFAULT_PARTITION_REQUESTS_TIMEOUT = TimeUnit.SECONDS.toMillis(15);

    /**
     * Stores the list of bootstrap nodes (where the cluster is).
     */
    private final List<InetSocketAddress> clusterAt;

    /**
     * Stores the generator for each DCP connection name.
     */
    private final ConnectionNameGenerator connectionNameGenerator;

    /**
     * provide username/password pairs.
     */
    private final CredentialsProvider credentialsProvider;

    /**
     * The name of the bucket.
     */
    private final String bucket;

    /**
     * DCP control params, optional.
     */
    private final DcpControl dcpControl;

    /**
     * The IO loops.
     */
    private final EventLoopGroup eventLoopGroup;

    /**
     * If the client instantiated the IO loops (it is then responsible for shutdown).
     */
    private final boolean eventLoopGroupIsPrivate;

    /**
     * If buffer pooling is enabled throughout the client.
     */
    private final boolean poolBuffers;

    /**
     * What the buffer ack watermark in percent should be.
     */
    private final int bufferAckWatermark;

    /**
     * The configured noop interval, in seconds
     */
    private final int noopIntervalSeconds;

    /**
     * User-attached data event handler.
     */
    private volatile DataEventHandler dataEventHandler;

    /**
     * User-attached control event handler.
     */
    private volatile ControlEventHandler controlEventHandler;

    /**
     * Time in milliseconds to wait for initial configuration during bootstrap.
     */
    private final long configProviderAttemptTimeout;

    /**
     * Time in milliseconds for all attempts to get configuration.
     */
    private final long configProviderTotalTimeout;

    /**
     * Delay strategy for configuration provider reconnection.
     */
    private final Delay configProviderReconnectDelay;

    /**
     * DCP socket connect single attempt timeout in milliseconds.
     */
    private final long dcpChannelAttemptTimeout;

    /**
     * DCP socket connect total timeout in milliseconds.
     */
    private final long dcpChannelTotalTimeout;

    /**
     * Delay strategy for configuration provider reconnection.
     */
    private final Delay dcpChannelsReconnectDelay;

    /**
     * Timeout for partition information requests
     */
    private final long partitionRequestsTimeout;

    private final EventBus eventBus;
    private final boolean sslEnabled;
    private final String sslKeystoreFile;
    private final String sslKeystorePassword;
    private final KeyStore sslKeystore;
    private final int bootstrapHttpDirectPort;
    private final int bootstrapHttpSslPort;
    private final FlowControlCallback flowControlCallback;
    private short[] vbuckets;
    private final String uuid;
    private final boolean dynamicConfigurationNodes;
    private final NetworkResolution networkResolution;

    /**
     * Creates a new environment based on the builder.
     *
     * @param builder
     *            the builder to build the environment.
     */
    private ClientEnvironment(final Builder builder) {
        connectionNameGenerator = builder.connectionNameGenerator;
        bucket = builder.bucket;
        credentialsProvider = builder.credentialsProvider;
        dcpControl = builder.dcpControl;
        eventLoopGroup = builder.eventLoopGroup;
        eventLoopGroupIsPrivate = builder.eventLoopGroupIsPrivate;
        bufferAckWatermark = builder.bufferAckWatermark;
        poolBuffers = builder.poolBuffers;

        if (builder.eventBus != null) {
            eventBus = builder.eventBus;
        } else {
            eventBus = new DefaultEventBus();
        }
        bootstrapHttpDirectPort = builder.bootstrapHttpDirectPort;
        bootstrapHttpSslPort = builder.bootstrapHttpSslPort;
        sslEnabled = builder.sslEnabled;
        sslKeystoreFile = builder.sslKeystoreFile;
        sslKeystorePassword = builder.sslKeystorePassword;
        sslKeystore = builder.sslKeystore;
        clusterAt = builder.clusterAt;
        vbuckets = builder.vbuckets;
        flowControlCallback = builder.flowControlCallback;
        // Timeouts, retries, and delays
        configProviderAttemptTimeout = builder.configProviderAttemptTimeout;
        configProviderTotalTimeout = builder.configProviderTotalTimeout;
        configProviderReconnectDelay = builder.configProviderReconnectDelay;
        dcpChannelAttemptTimeout = builder.dcpChannelAttemptTimeout;
        dcpChannelTotalTimeout = builder.dcpChannelTotalTimeout;
        dcpChannelsReconnectDelay = builder.dcpChannelsReconnectDelay;
        partitionRequestsTimeout = builder.partitionRequestsTimeout;
        uuid = builder.uuid;
        dynamicConfigurationNodes = builder.dynamicConfigurationNodes;
        networkResolution = builder.networkResolution;
        if (dcpControl != null) {
            String noopInterval = dcpControl().get(Names.SET_NOOP_INTERVAL);
            noopIntervalSeconds = noopInterval == null ? 0 : Integer.parseInt(noopInterval);
        } else {
            noopIntervalSeconds = 0;
        }
    }

    /**
     * Returns a new {@link Builder} to craft a {@link ClientEnvironment}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Lists the bootstrap nodes.
     */
    public List<InetSocketAddress> clusterAt() {
        return clusterAt;
    }

    /**
     * Returns the currently attached data event handler.
     */
    public DataEventHandler dataEventHandler() {
        return dataEventHandler;
    }

    /**
     * Returns the Flow Control Callback
     */
    public FlowControlCallback flowControlCallback() {
        return flowControlCallback;
    }

    /**
     * Returns the current attached control event handler.
     */
    public ControlEventHandler controlEventHandler() {
        return controlEventHandler;
    }

    /**
     * Returns the name generator used to identify DCP sockets.
     */
    public ConnectionNameGenerator connectionNameGenerator() {
        return connectionNameGenerator;
    }

    /**
     * Name of the bucket used.
     */
    public String bucket() {
        return bucket;
    }

    public String uuid() {
        return uuid;
    }

    public boolean dynamicConfigurationNodes() {
        return dynamicConfigurationNodes;
    }

    /**
     * Returns all DCP control params set, may be empty.
     */
    public DcpControl dcpControl() {
        return dcpControl;
    }

    /**
     * The watermark in percent for buffer acknowledgements.
     */
    public int ackWaterMark() {
        return bufferAckWatermark;
    }

    /**
     * Returns the currently attached event loop group for IO process.ing.
     */
    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    /**
     * Time in milliseconds to wait for first configuration during bootstrap.
     */
    public long configProviderAttemptTimeout() {
        return configProviderAttemptTimeout;
    }

    /**
     * Total time in milliseconds to get bucket configuration
     */
    public long configProviderTotalTimeout() {
        return configProviderTotalTimeout;
    }

    public short[] vbuckets() {
        return vbuckets;
    }

    public void vbuckets(short[] vbuckets) {
        this.vbuckets = vbuckets;
    }

    /**
     * Set/Override the data event handler.
     */
    public void setDataEventHandler(DataEventHandler dataEventHandler) {
        this.dataEventHandler = dataEventHandler;
    }

    /**
     * Set/Override the control event handler.
     */
    public void setControlEventHandler(ControlEventHandler controlEventHandler) {
        this.controlEventHandler = controlEventHandler;
    }

    /**
     * Set/Override the control event handler.
     */
    public void setSystemEventHandler(final SystemEventHandler systemEventHandler) {
        eventBus.subscribe(systemEventHandler);
    }

    /**
     * If buffer pooling is enabled.
     */
    public boolean poolBuffers() {
        return poolBuffers;
    }

    /**
     * Delay strategy for configuration provider reconnection.
     */
    public Delay configProviderReconnectDelay() {
        return configProviderReconnectDelay;
    }

    /**
     * Socket connect timeout in milliseconds.
     */
    public long dcpChannelAttemptTimeout() {
        return dcpChannelAttemptTimeout;
    }

    public long dcpChannelTotalTimeout() {
        return dcpChannelTotalTimeout;
    }

    public long partitionRequestsTimeout() {
        return partitionRequestsTimeout;
    }

    /**
     * Returns the event bus where events are broadcasted on and can be published to.
     */
    public EventBus eventBus() {
        return eventBus;
    }

    public int bootstrapHttpDirectPort() {
        return bootstrapHttpDirectPort;
    }

    public int bootstrapHttpSslPort() {
        return bootstrapHttpSslPort;
    }

    @Override
    public boolean sslEnabled() {
        return sslEnabled;
    }

    @Override
    public String sslKeystoreFile() {
        return sslKeystoreFile;
    }

    @Override
    public String sslKeystorePassword() {
        return sslKeystorePassword;
    }

    @Override
    public KeyStore sslKeystore() {
        return sslKeystore;
    }

    @Override
    public MemcachedHashingStrategy memcachedHashingStrategy() {
        // This is hardcoded, because memcached nodes do not support DCP anyway.
        return DefaultMemcachedHashingStrategy.INSTANCE;
    }

    public NetworkResolution networkResolution() {
        return networkResolution;
    }

    public static class Builder {
        private List<InetSocketAddress> clusterAt;
        private ConnectionNameGenerator connectionNameGenerator = DefaultConnectionNameGenerator.INSTANCE;
        private String bucket;
        private CredentialsProvider credentialsProvider;
        private DcpControl dcpControl;
        private EventLoopGroup eventLoopGroup;
        private boolean eventLoopGroupIsPrivate;
        private boolean poolBuffers;
        private int bootstrapHttpDirectPort = BOOTSTRAP_HTTP_DIRECT_PORT;
        private int bootstrapHttpSslPort = BOOTSTRAP_HTTP_SSL_PORT;
        private int bufferAckWatermark;
        private EventBus eventBus;
        private boolean sslEnabled = DEFAULT_SSL_ENABLED;
        private String sslKeystoreFile;
        private String sslKeystorePassword;
        private KeyStore sslKeystore;
        private short[] vbuckets;
        private FlowControlCallback flowControlCallback;
        private String uuid;
        private boolean dynamicConfigurationNodes = true;

        /*
         * Config Provider
         */
        private long configProviderAttemptTimeout = DEFAULT_CONFIG_PROVIDER_ATTEMPT_TIMEOUT;
        private long configProviderTotalTimeout = DEFAULT_CONFIG_PROVIDER_ATTEMPT_TIMEOUT;
        private Delay configProviderReconnectDelay = DEFAULT_CONFIG_PROVIDER_RECONNECT_DELAY;
        /*
         * DCP Connection
         */
        private long dcpChannelAttemptTimeout = DEFAULT_DCP_CHANNEL_ATTEMPT_TIMEOUT;
        private long dcpChannelTotalTimeout = DEFAULT_DCP_CHANNEL_TOTAL_TIMEOUT;
        private Delay dcpChannelsReconnectDelay = DEFAULT_DCP_CHANNELS_RECONNECT_DELAY;
        private long partitionRequestsTimeout = DEFAULT_PARTITION_REQUESTS_TIMEOUT;
        private NetworkResolution networkResolution;

        public Builder setClusterAt(List<InetSocketAddress> clusterAt) {
            this.clusterAt = clusterAt;
            return this;
        }

        public Builder setBufferAckWatermark(int watermark) {
            this.bufferAckWatermark = watermark;
            return this;
        }

        public Builder setConnectionNameGenerator(ConnectionNameGenerator connectionNameGenerator) {
            this.connectionNameGenerator = connectionNameGenerator;
            return this;
        }

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        public Builder setConfigProviderAttemptTimeout(long configProviderAttemptTimeout) {
            this.configProviderAttemptTimeout = configProviderAttemptTimeout;
            return this;
        }

        public Builder setConfigProviderTotalTimeout(long configProviderTotalTimeout) {
            this.configProviderTotalTimeout = configProviderTotalTimeout;
            return this;
        }

        public Builder setConfigProviderReconnectDelay(Delay configProviderReconnectDelay) {
            this.configProviderReconnectDelay = configProviderReconnectDelay;
            return this;
        }

        public Builder setDcpChannelsReconnectDelay(Delay dcpChannelsReconnectDelay) {
            this.dcpChannelsReconnectDelay = dcpChannelsReconnectDelay;
            return this;
        }

        public Builder setDcpChannelAttemptTimeout(long dcpChannelAttemptTimeout) {
            this.dcpChannelAttemptTimeout = dcpChannelAttemptTimeout;
            return this;
        }

        public Builder setDcpChannelTotalTimeout(long dcpChannelTotalTimeout) {
            this.dcpChannelTotalTimeout = dcpChannelTotalTimeout;
            return this;
        }

        public Builder setPartitionRequestsTimeout(long partitionRequestsTimeout) {
            this.partitionRequestsTimeout = partitionRequestsTimeout;
            return this;
        }

        public Builder setDcpControl(DcpControl dcpControl) {
            this.dcpControl = dcpControl;
            return this;
        }

        public Builder setEventLoopGroup(EventLoopGroup eventLoopGroup, boolean priv) {
            this.eventLoopGroup = eventLoopGroup;
            this.eventLoopGroupIsPrivate = priv;
            return this;
        }

        public Builder setBufferPooling(boolean pool) {
            this.poolBuffers = pool;
            return this;
        }

        public Builder setEventBus(EventBus eventBus) {
            this.eventBus = eventBus;
            return this;
        }

        /**
         * If SSL not enabled, sets the port to use for HTTP bootstrap
         * (default value {@value #BOOTSTRAP_HTTP_DIRECT_PORT}).
         */
        public Builder setBootstrapHttpDirectPort(final int bootstrapHttpDirectPort) {
            this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
            return this;
        }

        /**
         * If SSL enabled, sets the port to use for HTTP bootstrap
         * (default value {@value #BOOTSTRAP_HTTP_SSL_PORT}).
         */
        public Builder setBootstrapHttpSslPort(final int bootstrapHttpSslPort) {
            this.bootstrapHttpSslPort = bootstrapHttpSslPort;
            return this;
        }

        /**
         * Set if SSL should be enabled (default value {@value #DEFAULT_SSL_ENABLED}).
         * If true, also set {@link #setSslKeystoreFile(String)} and {@link #setSslKeystorePassword(String)}.
         */
        public Builder setSslEnabled(final boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return this;
        }

        /**
         * Defines the location of the SSL Keystore file (default value null, none).
         *
         * You can either specify a file or the keystore directly via {@link #setSslKeystore(KeyStore)}. If the explicit
         * keystore is used it takes precedence over the file approach.
         */
        public Builder setSslKeystoreFile(final String sslKeystoreFile) {
            this.sslKeystoreFile = sslKeystoreFile;
            return this;
        }

        /**
         * Sets the SSL Keystore password to be used with the Keystore file (default value null, none).
         *
         * @see #setSslKeystoreFile(String)
         */
        public Builder setSslKeystorePassword(final String sslKeystorePassword) {
            this.sslKeystorePassword = sslKeystorePassword;
            return this;
        }

        /**
         * Sets the SSL Keystore directly and not indirectly via filepath.
         *
         * You can either specify a file or the keystore directly via {@link #setSslKeystore(KeyStore)}. If the explicit
         * keystore is used it takes precedence over the file approach.
         *
         * @param sslKeystore
         *            the keystore to use.
         */
        public Builder setSslKeystore(final KeyStore sslKeystore) {
            this.sslKeystore = sslKeystore;
            return this;
        }

        public Builder setVbuckets(final short[] vbuckets) {
            this.vbuckets = vbuckets;
            return this;
        }

        public Builder setFlowControlCallback(final FlowControlCallback flowControlCallback) {
            this.flowControlCallback = flowControlCallback;
            return this;
        }

        public Builder setUuid(final String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder setDynamicConfigurationNodes(final boolean dynamicConfigurationNodes) {
            this.dynamicConfigurationNodes = dynamicConfigurationNodes;
            return this;
        }

        public ClientEnvironment build() {
            int defaultConfigPort = sslEnabled ? bootstrapHttpSslPort : bootstrapHttpDirectPort;
            for (int i = 0; i < clusterAt.size(); i++) {
                InetSocketAddress node = clusterAt.get(i);
                if (node.getPort() == 0) {
                    clusterAt.set(i, new InetSocketAddress(node.getHostString(), defaultConfigPort));
                } else if (node.getAddress() == null) {
                    clusterAt.set(i, new InetSocketAddress(node.getHostString(), node.getPort()));
                }
            }
            return new ClientEnvironment(this);
        }

        public Builder setNetworkResolution(NetworkResolution networkResolution) {
            this.networkResolution = networkResolution;
            return this;
        }
    }

    /**
     * Shut down this stateful environment.
     *
     * Note that it will only release/terminate resources which are owned by the client,
     * especially if a custom event loop group is passed in it needs to be shut down
     * separately.
     *
     * @return a {@link Completable} indicating completion of the shutdown process.
     */
    public Completable shutdown() {
        Observable<Boolean> loopShutdown = Observable.empty();

        if (eventLoopGroupIsPrivate) {
            loopShutdown = Completable.create(subscriber -> eventLoopGroup
                    .shutdownGracefully(0, 10, TimeUnit.MILLISECONDS).addListener(future -> {
                        if (future.isSuccess()) {
                            subscriber.onCompleted();
                        } else {
                            subscriber.onError(future.cause());
                        }
                    })).toObservable();
        }
        return loopShutdown.toCompletable();
    }

    @Override
    public String toString() {
        return "ClientEnvironment{" + "clusterAt=" + clusterAt + ", connectionNameGenerator="
                + connectionNameGenerator.getClass().getSimpleName() + ", bucket='" + bucket + '\'' + ", passwordSet="
                + (credentialsProvider != null) + ", dcpControl=" + dcpControl + ", eventLoopGroup="
                + eventLoopGroup.getClass().getSimpleName() + ", eventLoopGroupIsPrivate=" + eventLoopGroupIsPrivate
                + ", poolBuffers=" + poolBuffers + ", bufferAckWatermark=" + bufferAckWatermark
                + ", dcpChannelAttemptTimeout=" + dcpChannelAttemptTimeout + ", dcpChannelTotalTimeout="
                + dcpChannelTotalTimeout + ", dcpChannelsReconnectDelay=" + dcpChannelsReconnectDelay
                + ", configProviderAttemptTimeout=" + configProviderAttemptTimeout + ", configProviderTotalTimeout="
                + configProviderTotalTimeout + ", configProviderReconnectDelay=" + configProviderReconnectDelay
                + ", sslEnabled=" + sslEnabled + ", sslKeystoreFile='" + sslKeystoreFile + '\''
                + ", sslKeystorePassword=" + (sslKeystorePassword != null && !sslKeystorePassword.isEmpty())
                + ", sslKeystore=" + sslKeystore + '}';
    }

    public Delay dcpChannelsReconnectDelay() {
        return dcpChannelsReconnectDelay;
    }

    public CredentialsProvider credentialsProvider() {
        return credentialsProvider;
    }

    public int getNoopIntervalSeconds() {
        return noopIntervalSeconds;
    }

    public int getDeadConnectionDetectionIntervalSeconds() {
        return getNoopIntervalSeconds() * 2;
    }
}

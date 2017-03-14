/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

import java.net.InetAddress;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.List;

import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.conductor.ConfigProvider;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.events.EventBus;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;

/**
 * Builder object to customize the {@link Client} creation.
 */
public class ClientBuilder {
    private List<String> hostnames = Arrays.asList(InetAddress.getLoopbackAddress().getHostAddress());
    private EventLoopGroup eventLoopGroup;
    private String bucket = "default";
    private String password = "";
    private ConnectionNameGenerator connectionNameGenerator = DefaultConnectionNameGenerator.INSTANCE;
    private DcpControl dcpControl = new DcpControl();
    private ConfigProvider configProvider = null;
    private int bufferAckWatermark;
    private boolean poolBuffers = true;
    private long connectTimeout = ClientEnvironment.DEFAULT_SOCKET_CONNECT_TIMEOUT;
    private long bootstrapTimeout = ClientEnvironment.DEFAULT_BOOTSTRAP_TIMEOUT;
    private long socketConnectTimeout = ClientEnvironment.DEFAULT_SOCKET_CONNECT_TIMEOUT;
    private Delay configProviderReconnectDelay = ClientEnvironment.DEFAULT_CONFIG_PROVIDER_RECONNECT_DELAY;
    private int configProviderReconnectMaxAttempts = ClientEnvironment.DEFAULT_CONFIG_PROVIDER_RECONNECT_MAX_ATTEMPTS;
    private int dcpChannelsReconnectMaxAttempts = ClientEnvironment.DEFAULT_DCP_CHANNELS_RECONNECT_MAX_ATTEMPTS;
    private Delay dcpChannelsReconnectDelay = ClientEnvironment.DEFAULT_DCP_CHANNELS_RECONNECT_DELAY;
    private EventBus eventBus;
    private boolean sslEnabled = ClientEnvironment.DEFAULT_SSL_ENABLED;
    private String sslKeystoreFile;
    private String sslKeystorePassword;
    private KeyStore sslKeystore;
    private int configPort = ClientEnvironment.BOOTSTRAP_HTTP_DIRECT_PORT;
    private int sslConfigPort = ClientEnvironment.BOOTSTRAP_HTTP_SSL_PORT;
    private int dcpPort = ClientEnvironment.DCP_DIRECT_PORT;
    private int sslDcpPort = ClientEnvironment.DCP_SSL_PORT;
    private short[] vbuckets;

    /**
     * The buffer acknowledge watermark in percent.
     *
     * @param watermark
     *            between 0 and 100, needs to be > 0 if flow control is enabled.
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder bufferAckWatermark(int watermark) {
        if (watermark > 100 || watermark < 0) {
            throw new IllegalArgumentException(
                    "The bufferAckWatermark is percents, so it needs to be between" + " 0 and 100");
        }
        this.bufferAckWatermark = watermark;
        return this;
    }

    /**
     * The hostnames to bootstrap against.
     *
     * @param hostnames
     *            seed nodes.
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder hostnames(final List<String> hostnames) {
        this.hostnames = hostnames;
        return this;
    }

    /**
     * The hostnames to bootstrap against.
     *
     * @param hostnames
     *            seed nodes.
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder hostnames(String... hostnames) {
        return hostnames(Arrays.asList(hostnames));
    }

    /**
     * Sets a custom event loop group, this is needed if more than one client is initialized and
     * runs at the same time to keep the IO threads efficient and in bounds.
     *
     * @param eventLoopGroup
     *            the group that should be used.
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder eventLoopGroup(final EventLoopGroup eventLoopGroup) {
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
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder bucket(final String bucket) {
        this.bucket = bucket;
        return this;
    }

    /**
     * The password of the bucket to use.
     *
     * @param password
     *            the password.
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder password(final String password) {
        this.password = password;
        return this;
    }

    /**
     * If specific names for DCP connections should be generated, a custom one can be provided.
     *
     * @param connectionNameGenerator
     *            custom generator.
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder connectionNameGenerator(final ConnectionNameGenerator connectionNameGenerator) {
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
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder controlParam(final DcpControl.Names name, Object value) {
        this.dcpControl.put(name, value.toString());
        return this;
    }

    /**
     * A custom configuration provider can be shared and passed in across clients. use with care!
     *
     * @param configProvider
     *            the custom config provider.
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder configProvider(final ConfigProvider configProvider) {
        this.configProvider = configProvider;
        return this;
    }

    /**
     * If buffer pooling should be enabled (yes by default).
     *
     * @param pool
     *            enable or disable buffer pooling.
     * @return this {@link ClientBuilder} for nice chainability.
     */
    public ClientBuilder poolBuffers(final boolean pool) {
        this.poolBuffers = pool;
        return this;
    }

    /**
     * Sets a custom socket connect timeout.
     *
     * @param socketConnectTimeout
     *            the socket connect timeout in milliseconds.
     */
    public ClientBuilder socketConnectTimeout(long socketConnectTimeout) {
        this.socketConnectTimeout = socketConnectTimeout;
        return this;
    }

    /**
     * Time to wait for first configuration during bootstrap.
     *
     * @param bootstrapTimeout
     *            time in milliseconds.
     */
    public ClientBuilder bootstrapTimeout(long bootstrapTimeout) {
        this.bootstrapTimeout = bootstrapTimeout;
        return this;
    }

    /**
     * Time to wait configuration provider socket to connect.
     *
     * @param connectTimeout
     *            time in milliseconds.
     */
    public ClientBuilder connectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /**
     * Delay between retry attempts for configuration provider
     *
     * @param configProviderReconnectDelay
     */
    public ClientBuilder configProviderReconnectDelay(Delay configProviderReconnectDelay) {
        this.configProviderReconnectDelay = configProviderReconnectDelay;
        return this;
    }

    /**
     * The maximum number of reconnect attempts for configuration provider
     *
     * @param configProviderReconnectMaxAttempts
     */
    public ClientBuilder configProviderReconnectMaxAttempts(int configProviderReconnectMaxAttempts) {
        this.configProviderReconnectMaxAttempts = configProviderReconnectMaxAttempts;
        return this;
    }

    /**
     * The maximum number of reconnect attempts for DCP channels
     *
     * @param dcpChannelsReconnectMaxAttempts
     */
    public ClientBuilder dcpChannelsReconnectMaxAttempts(int dcpChannelsReconnectMaxAttempts) {
        this.dcpChannelsReconnectMaxAttempts = dcpChannelsReconnectMaxAttempts;
        return this;
    }

    /**
     * Delay between retry attempts for DCP channels
     *
     * @param dcpChannelsReconnectDelay
     */
    public ClientBuilder dcpChannelsReconnectDelay(Delay dcpChannelsReconnectDelay) {
        this.dcpChannelsReconnectDelay = dcpChannelsReconnectDelay;
        return this;
    }

    /**
     * Sets the event bus to an alternative implementation.
     *
     * This setting should only be tweaked in advanced cases.
     */
    public ClientBuilder eventBus(final EventBus eventBus) {
        this.eventBus = eventBus;
        return this;
    }

    /**
     * Set if SSL should be enabled (default value {@value ClientEnvironment#DEFAULT_SSL_ENABLED}).
     * If true, also set {@link #sslKeystoreFile(String)} and {@link #sslKeystorePassword(String)}.
     */
    public ClientBuilder sslEnabled(final boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
        return this;
    }

    /**
     * Defines the location of the SSL Keystore file (default value null, none).
     *
     * You can either specify a file or the keystore directly via {@link #sslKeystore(KeyStore)}. If the explicit
     * keystore is used it takes precedence over the file approach.
     */
    public ClientBuilder sslKeystoreFile(final String sslKeystoreFile) {
        this.sslKeystoreFile = sslKeystoreFile;
        return this;
    }

    /**
     * Sets the SSL Keystore password to be used with the Keystore file (default value null, none).
     *
     * @see #sslKeystoreFile(String)
     */
    public ClientBuilder sslKeystorePassword(final String sslKeystorePassword) {
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
    public ClientBuilder sslKeystore(final KeyStore sslKeystore) {
        this.sslKeystore = sslKeystore;
        return this;
    }

    /**
     * Sets the Port that will be used to get bucket configurations.
     *
     * @param configPort
     *            the port to use
     */
    public ClientBuilder configPort(final int configPort) {
        this.configPort = configPort;
        return this;
    }

    /**
     * Sets the Port that will be used to get bucket configurations for encrypted connections
     *
     * @param sslConfigPort
     *            the port to use
     */
    public ClientBuilder sslConfigPort(final int sslConfigPort) {
        this.sslConfigPort = sslConfigPort;
        return this;
    }

    /**
     * Sets the Port that will be used for DCP streams
     *
     * @param dcpPort
     *            the port to use
     */
    public ClientBuilder dcpPort(final int dcpPort) {
        this.dcpPort = dcpPort;
        return this;
    }

    /**
     * Sets the Port that will be used for encrypted DCP streams
     *
     * @param sslDcpPort
     *            the port to use
     */
    public ClientBuilder sslDcpPort(final int sslDcpPort) {
        this.sslDcpPort = sslDcpPort;
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

    public List<String> hostnames() {
        return hostnames;
    }

    public ConnectionNameGenerator connectionNameGenerator() {
        return connectionNameGenerator;
    }

    public String bucket() {
        return bucket;
    }

    public String password() {
        return password;
    }

    public DcpControl dcpControl() {
        return dcpControl;
    }

    public int bufferAckWatermark() {
        return bufferAckWatermark;
    }

    public long connectTimeout() {
        return connectTimeout;
    }

    public boolean poolBuffers() {
        return poolBuffers;
    }

    public long bootstrapTimeout() {
        return bootstrapTimeout;
    }

    public long socketConnectTimeout() {
        return socketConnectTimeout;
    }

    public Delay configProviderReconnectDelay() {
        return configProviderReconnectDelay;
    }

    public int configProviderReconnectMaxAttempts() {
        return configProviderReconnectMaxAttempts;
    }

    public Delay dcpChannelsReconnectDelay() {
        return dcpChannelsReconnectDelay;
    }

    public int dcpChannelsReconnectMaxAttempts() {
        return dcpChannelsReconnectMaxAttempts;
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

    public int dcpPort() {
        return dcpPort;
    }

    public int sslDcpPort() {
        return sslDcpPort;
    }

    public ConfigProvider configProvider() {
        return configProvider;
    }

    public ClientBuilder vbuckets(final short[] vbuckets) {
        this.vbuckets = vbuckets;
        return this;
    }

    public short[] vbuckets() {
        return vbuckets;
    }
}
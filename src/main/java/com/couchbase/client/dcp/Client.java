/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.conductor.Conductor;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StreamRequest;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;

import rx.Completable;
import rx.Observable;

/**
 * This {@link Client} provides the main API to configure and use the DCP client.
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
    public Client(ClientBuilder builder) {
        EventLoopGroup eventLoopGroup =
                builder.eventLoopGroup() == null ? new NioEventLoopGroup() : builder.eventLoopGroup();

        env = ClientEnvironment.builder().setClusterAt(builder.hostnames())
                .setConnectionNameGenerator(builder.connectionNameGenerator()).setBucket(builder.bucket())
                .setPassword(builder.password()).setDcpControl(builder.dcpControl())
                .setEventLoopGroup(eventLoopGroup, builder.eventLoopGroup() == null)
                .setBufferAckWatermark(builder.bufferAckWatermark()).setBufferPooling(builder.poolBuffers())
                .setConnectTimeout(builder.connectTimeout()).setBootstrapTimeout(builder.bootstrapTimeout())
                .setSocketConnectTimeout(builder.socketConnectTimeout())
                .setConfigProviderReconnectDelay(builder.configProviderReconnectDelay())
                .setConfigProviderReconnectMaxAttempts(builder.configProviderReconnectMaxAttempts())
                .setDcpChannelsReconnectDelay(builder.dcpChannelsReconnectDelay())
                .setDcpChannelsReconnectMaxAttempts(builder.dcpChannelsReconnectMaxAttempts())
                .setEventBus(builder.eventBus()).setSslEnabled(builder.sslEnabled())
                .setSslKeystoreFile(builder.sslKeystoreFile()).setSslKeystorePassword(builder.sslKeystorePassword())
                .setSslKeystore(builder.sslKeystore()).setBootstrapHttpDirectPort(builder.configPort())
                .setBootstrapHttpSslPort(builder.sslConfigPort()).setDcpDirectPort(builder.dcpPort())
                .setDcpSslPort(builder.sslDcpPort()).setVbuckets(builder.vbuckets()).build();

        ackEnabled = env.dcpControl().ackEnabled();
        if (ackEnabled && env.ackWaterMark() == 0) {
            throw new IllegalArgumentException("The bufferAckWatermark needs to be set if bufferAck is enabled.");
        }

        conductor = new Conductor(env, builder.configProvider());
        LOGGER.info("Environment Configuration Used: {}", env);

    }

    /**
     * Allows to configure the {@link Client} before bootstrap through a {@link ClientBuilder}.
     *
     * @return the builder to configure the client.
     */
    public static ClientBuilder builder() {
        return new ClientBuilder();
    }

    /**
     * Get the current sequence numbers from all partitions.
     *
     * Each element emitted into the observable has two elements. The first element is the partition and
     * the second element is its sequence number.
     *
     * @return an {@link Observable} of sequence number arrays.
     */
    public void getSequenceNumbers() {
        conductor.getSeqnos();
    }

    /**
     * Returns the current {@link SessionState}, useful for persistence and inspection.
     *
     * @return the current session state.
     */
    public SessionState sessionState() {
        return conductor.sessionState();
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
    public void dataEventHandler(final DataEventHandler dataEventHandler) {
        env.setDataEventHandler(event -> {
            if (DcpMutationMessage.is(event)) {
                short partition = DcpMutationMessage.partition(event);
                PartitionState ps = sessionState().get(partition);
                ps.setSeqno(DcpMutationMessage.bySeqno(event));
            } else if (DcpDeletionMessage.is(event)) {
                short partition = DcpDeletionMessage.partition(event);
                PartitionState ps = sessionState().get(partition);
                ps.setSeqno(DcpDeletionMessage.bySeqno(event));
            } else if (DcpExpirationMessage.is(event)) {
                short partition = DcpExpirationMessage.partition(event);
                PartitionState ps = sessionState().get(partition);
                ps.setSeqno(DcpExpirationMessage.bySeqno(event));
            }
            dataEventHandler.onEvent(event);
        });
    }

    /**
     * Initializes the underlying connections (not the streams) and sets up everything as needed.
     *
     * @return a {@link Completable} signaling that the connect phase has been completed or failed.
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
     * @return a {@link Completable} signaling that the disconnect phase has been completed or failed.
     * @throws InterruptedException
     */
    public synchronized void disconnect() throws InterruptedException {
        conductor.stop();
        env.shutdown();
    }

    /**
     * Start DCP streams based on the initialized state for the given partition IDs (vbids).
     *
     * If no ids are provided, all initialized partitions will be started.
     *
     * @param vbids
     *            the partition ids (0-indexed) to start streaming for.
     * @return a {@link Completable} indicating that streaming has started or failed.
     * @throws InterruptedException
     */
    public void startStreaming(short... vbids) throws InterruptedException {
        validateStream();
        int numPartitions = numPartitions();
        final List<Short> partitions = partitionsForVbids(numPartitions, vbids);
        ensureInitialized(partitions);
        LOGGER.info("Starting to Stream for " + partitions.size() + " partitions");
        LOGGER.info("Stream start against partitions: {}", partitions);
        for (short vbid : vbids) {
            PartitionState ps = sessionState().get(vbid);
            LOGGER.warn("Starting partition " + vbid + " from the starting point " + ps.getStreamRequest());
        }
        for (short partition : partitions) {
            PartitionState partitionState = sessionState().get(partition);
            StreamRequest request = partitionState.getStreamRequest();
            conductor.startStreamForPartition(partition, request.getVbucketUuid(), request.getStartSeqno(),
                    request.getEndSeqno(), request.getSnapshotStartSeqno(), request.getSnapshotEndSeqno());
        }
    }

    private void ensureInitialized(List<Short> partitions) throws InterruptedException {
        SessionState state = sessionState();
        List<Short> nonInitialized = new ArrayList<>();
        for (short partition : partitions) {
            PartitionState ps = state.get(partition);
            if (ps.getStreamRequest() == null) {
                if (!ps.getFailoverLog().isEmpty()) {
                    ps.prepareNextStreamRequest();
                } else {
                    nonInitialized.add(ps.vbid());
                }
            }
        }
        failoverLogs(nonInitialized);
        for (short sh : nonInitialized) {
            PartitionState ps = state.get(sh);
            ps.prepareNextStreamRequest();
        }
    }

    /**
     * Stop DCP streams for the given partition IDs (vbids).
     *
     * If no ids are provided, all partitions will be stopped. Note that you can also use this to "pause" streams
     * if {@link #startStreaming(Short...)} is called later - since the session state is persisted and streaming
     * will resume from the current position.
     *
     * @param vbids
     *            the partition ids (0-indexed) to stop streaming for.
     * @return a {@link Completable} indicating that streaming has stopped or failed.
     * @throws InterruptedException
     */
    public void stopStreaming(short... vbids) throws InterruptedException {
        List<Short> partitions = partitionsForVbids(numPartitions(), vbids);
        LOGGER.info("Stopping to Stream for " + partitions.size() + " partitions");
        LOGGER.debug("Stream stop against partitions: {}", partitions);
        for (short partition : partitions) {
            conductor.stopStreamForPartition(partition);
        }
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
    private static List<Short> partitionsForVbids(int numPartitions, short... vbids) {
        List<Short> partitions = new ArrayList<>();
        if (vbids.length > 0) {
            partitions = new ArrayList<>(vbids.length);
            for (short sh : vbids) {
                partitions.add(sh);
            }
        } else {
            for (short i = 0; i < numPartitions; i++) {
                partitions.add(i);
            }
        }
        Collections.sort(partitions);
        return partitions;
    }

    /**
     * Helper method to return the failover logs for the given partitions (vbids).
     *
     * If the list is empty, the failover logs for all partitions will be returned. Note that the returned
     * ByteBufs can be analyzed using the {@link DcpFailoverLogResponse} flyweight.
     *
     * @param vbids
     *            the partitions to return the failover logs from.
     * @return an {@link Observable} containing all failover logs.
     * @throws InterruptedException
     */
    public void failoverLogs(short... vbids) throws InterruptedException {
        List<Short> partitions = partitionsForVbids(numPartitions(), vbids);
        LOGGER.debug("Asking for failover logs on partitions {}", partitions);
        for (short partition : partitions) {
            conductor.getFailoverLog(partition);
        }
    }

    private void failoverLogs(List<Short> nonInitialized) throws InterruptedException {
        short[] vbids = new short[nonInitialized.size()];
        for (int i = 0; i < nonInitialized.size(); i++) {
            vbids[i] = nonInitialized.get(i);
        }
        failoverLogs(vbids);
    }

    public void getFailoverLogs() throws InterruptedException {
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

    /**
     * Returns true if the stream for the given partition id is currently open.
     *
     * @param vbid
     *            the partition id.
     * @return true if it is open, false otherwise.
     */
    public boolean streamIsOpen(short vbid) {
        return conductor.streamIsOpen(vbid);
    }

    /**
     * Acknowledge bytes read if DcpControl.Names.CONNECTION_BUFFER_SIZE is set on bootstrap.
     *
     * Note that acknowledgement will be stored but most likely not sent to the server immediately to save network
     * overhead. Instead, depending on the value set through {@link ClientBuilder#bufferAckWatermark(int)} in percent
     * the client will automatically determine when to send the message (when the watermark is reached).
     *
     * This method can always be called even if not enabled, if not enabled on bootstrap it will short-circuit.
     *
     * @param vbid
     *            the partition id.
     * @param numBytes
     *            the number of bytes to acknowledge.
     */
    public void acknowledgeBuffer(int vbid, int numBytes) {
        if (!ackEnabled) {
            return;
        }
        conductor.acknowledgeBuffer((short) vbid, numBytes);
    }

    /**
     * Acknowledge bytes read if DcpControl.Names.CONNECTION_BUFFER_SIZE is set on bootstrap.
     *
     * This method is a convenience method which extracts the partition ID and the number of bytes to
     * acknowledge from the message. Make sure to only pass in legible buffers, coming from messages that are
     * ack'able, especially mutations, expirations and deletions.
     *
     * This method can always be called even if not enabled, if not enabled on bootstrap it will short-circuit.
     *
     * @param buffer
     *            the message to acknowledge.
     */
    public void acknowledgeBuffer(ByteBuf buffer) {
        acknowledgeBuffer(MessageUtil.getVbucket(buffer), buffer.readableBytes());
    }

    public CouchbaseBucketConfig config() {
        return conductor.config();
    }

    public void establishDcpConnections() throws Throwable {
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

    public PartitionState getState(short vbid) {
        return conductor.sessionState().get(vbid);
    }
}

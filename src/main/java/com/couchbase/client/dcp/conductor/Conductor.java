/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.conductor;

import static com.couchbase.client.dcp.util.retry.RetryBuilder.anyOf;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.events.FailedToAddNodeEvent;
import com.couchbase.client.dcp.events.FailedToRemoveNodeEvent;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.util.retry.RetryBuilder;
import com.couchbase.client.deps.io.netty.util.internal.ConcurrentSet;

import rx.Completable;
import rx.CompletableSubscriber;
import rx.Observable;
import rx.Subscription;

public class Conductor {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Conductor.class);

    private final ConfigProvider configProvider;
    private final Set<DcpChannel> channels;
    private volatile boolean stopped = true;
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

    public Completable connect() {
        stopped = false;
        return configProvider.refresh().doOnCompleted(() -> createSession(configProvider.config()));
    }

    /**
     * Returns true if all channels and the config provider are in a disconnected state.
     */
    public boolean disconnected() {
        for (DcpChannel channel : channels) {
            if (!channel.isState(LifecycleState.DISCONNECTED)) {
                return false;
            }
        }
        return true;
    }

    public Completable stop() {
        LOGGER.debug("Instructed to shutdown.");
        stopped = true;
        return Observable.from(channels).flatMap(dcpChannel -> dcpChannel.disconnect().toObservable()).toCompletable()
                .doOnCompleted(() -> LOGGER.info("Shutdown complete."));
    }

    /**
     * Returns the total number of partitions.
     */
    public int numberOfPartitions() {
        return configProvider.config().numberOfPartitions();
    }

    public Completable getSeqnos() {
        List<Completable> completables = new ArrayList<>();
        for (DcpChannel channel : channels) {
            completables.add(getSeqnosForChannel(channel));
        }
        return Completable.concat(completables);
    }

    @SuppressWarnings("unchecked")
    private Completable getSeqnosForChannel(final DcpChannel dcpChannel) {
        return dcpChannel.getSeqnos()
                .retryWhen(anyOf(NotConnectedException.class).max(Integer.MAX_VALUE)
                        .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
                        .doOnRetry((integer, throwable, aLong, timeUnit) -> LOGGER
                                .debug("Rescheduling get Seqnos for channel {}, not connected (yet).", dcpChannel))
                        .build());
    }

    @SuppressWarnings("unchecked")
    public Completable getFailoverLog(final short partition) {
        return masterChannelByPartition(partition).getFailoverLog(partition)
                .retryWhen(anyOf(NotConnectedException.class).max(Integer.MAX_VALUE)
                        .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
                        .doOnRetry((integer, throwable, aLong, timeUnit) -> LOGGER
                                .debug("Rescheduling Get Failover Log for vbid {}, not connected (yet).", partition))
                        .build());
    }

    @SuppressWarnings("unchecked")
    public Completable startStreamForPartition(final short partition, final long vbuuid, final long startSeqno,
            final long endSeqno, final long snapshotStartSeqno, final long snapshotEndSeqno) {
        return Observable.just(partition).map(aShort -> masterChannelByPartition(partition))
                .flatMap(channel -> channel
                        .openStream(partition, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno)
                        .toObservable())
                .retryWhen(anyOf(NotConnectedException.class).max(Integer.MAX_VALUE)
                        .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
                        .doOnRetry((integer, throwable, aLong, timeUnit) -> LOGGER
                                .debug("Rescheduling Stream Start for vbid {}, not connected (yet).", partition))
                        .build())
                .toCompletable();
    }

    public Completable stopStreamForPartition(final short partition) {
        if (streamIsOpen(partition)) {
            DcpChannel channel = masterChannelByPartition(partition);
            return channel.closeStream(partition);
        } else {
            return Completable.complete();
        }
    }

    public boolean streamIsOpen(final short partition) {
        DcpChannel channel = masterChannelByPartition(partition);
        return channel.streamIsOpen(partition);
    }

    public void acknowledgeBuffer(final short partition, int numBytes) {
        DcpChannel channel = masterChannelByPartition(partition);
        channel.acknowledgeBuffer(numBytes);
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

        throw new IllegalStateException("No DcpChannel found for partition " + partition);
    }

    private void createSession(CouchbaseBucketConfig config) {
        if (sessionState == null) {
            sessionState = new SessionState(config.numberOfPartitions());
        }
    }

    private Completable reconfigure() {
        List<InetAddress> toAdd = new ArrayList<>();
        List<DcpChannel> toRemove = new ArrayList<>();
        List<Completable> connects = new ArrayList<>();
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
            connects.add(add(add));
        }
        return Completable.concat(connects);
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
            return; // we only care about kv nodes
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

    private Completable add(final InetAddress node) {
        // noinspection SuspiciousMethodCalls: channel proxies equals/hashcode to its address
        if (channels.contains(node)) { // Are you serious??
            return Completable.complete();
        }
        LOGGER.debug("Adding DCP Channel against {}", node);
        final DcpChannel channel = new DcpChannel(node, env, this);
        channels.add(channel);
        Completable connectCompletable = channel.connect()
                .retryWhen(RetryBuilder.anyMatches(t -> !stopped).max(env.dcpChannelsReconnectMaxAttempts())
                        .delay(env.dcpChannelsReconnectDelay()).doOnRetry((integer, throwable, aLong,
                                timeUnit) -> LOGGER.debug("Rescheduling Node reconnect for DCP channel {}", node))
                        .build());
        connectCompletable.subscribe(new CompletableSubscriber() {
            @Override
            public void onCompleted() {
                LOGGER.debug("Completed Node connect for DCP channel {}", node);
            }

            @Override
            public void onError(Throwable e) {
                LOGGER.warn("Got error during connect (maybe retried) for node {}" + node, e);
                if (env.eventBus() != null) {
                    env.eventBus().publish(new FailedToAddNodeEvent(node, e));
                }
            }

            @Override
            public void onSubscribe(Subscription d) {
                // ignored.
            }
        });
        return connectCompletable;
    }

    private Completable remove(final DcpChannel node) {
        if (channels.remove(node)) {
            LOGGER.debug("Removing DCP Channel against {}", node);
            Completable completable = node.disconnect();
            completable.subscribe(new CompletableSubscriber() {
                @Override
                public void onCompleted() {
                    LOGGER.debug("Channel remove notified as complete for {}", node.hostname());
                }

                @Override
                public void onError(Throwable e) {
                    LOGGER.warn("Got error during Node removal for node {}" + node.hostname(), e);
                    if (env.eventBus() != null) {
                        env.eventBus().publish(new FailedToRemoveNodeEvent(node.hostname(), e));
                    }
                }

                @Override
                public void onSubscribe(Subscription d) {
                    // ignored.
                }
            });
            return completable;
        }
        return Completable.complete();
    }

    public SessionState getSessionState() {
        return sessionState;
    }

    public void setSessionState(SessionState sessionState) {
        this.sessionState = sessionState;
    }

    public void maybeMovePartition(short vbid) {
        LOGGER.log(CouchbaseLogLevel.WARN, "This could be a stream end for partition " + vbid);
    }

    public CouchbaseBucketConfig config() {
        return configProvider.config();
    }

    public Completable establishDcpConnections() {
        return reconfigure();
    }

    public DcpChannel getChannel(short vbid) {
        return masterChannelByPartition(vbid);
    }

    public void reset() {
    }
}

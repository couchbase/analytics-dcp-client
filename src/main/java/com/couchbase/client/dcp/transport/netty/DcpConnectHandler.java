/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.transport.netty;

import java.nio.charset.StandardCharsets;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.error.BucketNotFoundException;
import com.couchbase.client.dcp.message.DcpOpenConnectionRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.MemcachedStatus;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

/**
 * Opens the DCP connection on the channel and once established removes itself.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpConnectHandler extends ConnectInterceptingHandler<ByteBuf> {

    /**
     * MEMCACHED Status indicating a success
     */
    private static final byte SUCCESS = 0x00;

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpConnectHandler.class);

    private static final byte VERSION = 0;
    private static final byte HELO = 1;
    private static final byte SELECT = 2;
    private static final byte OPEN = 3;
    private static final byte REMOVE = 4;

    /**
     * The generated connection name, set fresh once a channel becomes active.
     */
    private final ByteBuf connectionName;
    private final String bucket;
    private byte step = VERSION;

    /**
     * Creates a new connect handler.
     *
     * @param environment
     *            the generator of the connection names.
     * @param dcpChannel
     */
    DcpConnectHandler(final ClientEnvironment environment, DcpChannel dcpChannel) {
        bucket = environment.bucket();
        final String connectionNameString = environment.connectionNameGenerator().name();
        connectionName = Unpooled.copiedBuffer(connectionNameString, CharsetUtil.UTF_8);
        dcpChannel.setConnectionName(connectionNameString);
    }

    /**
     * Once the channel becomes active, sends the initial open connection request.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        ByteBuf request = ctx.alloc().buffer();
        Version.init(request);
        ctx.writeAndFlush(request);
    }

    /**
     * Once we get a response from the connect request, check if it is successful and complete/fail the connect
     * phase accordingly.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        short status = MessageUtil.getStatus(msg);
        if (status == SUCCESS) {
            step++;
            switch (step) {
                case HELO:
                    helo(ctx, msg);
                    break;
                case SELECT:
                    // Select bucket
                    ByteBuf request = ctx.alloc().buffer();
                    BucketSelectionRequest.init(request, bucket);
                    ctx.writeAndFlush(request);
                    break;
                case OPEN:
                    // Open Connection
                    openConnection(ctx);
                    break;
                case REMOVE:
                    remove(ctx);
                    break;
                default:
                    originalPromise().setFailure(new IllegalStateException("Unidentified DcpConnection step " + step));
                    break;
            }
        } else {
            Exception failure;
            if (step == SELECT && status == MemcachedStatus.NOT_FOUND) {
                failure = new BucketNotFoundException(bucket);
            } else {
                failure = new IllegalStateException(
                        "Could not open DCP Connection to " + ctx.channel().remoteAddress() + ": Failed in the "
                                + toString(step) + " step, response status is " + MemcachedStatus.toString(status));
            }
            originalPromise().setFailure(failure);
        }
    }

    private void remove(ChannelHandlerContext ctx) {
        ctx.pipeline().remove(this);
        originalPromise().setSuccess();
        ctx.fireChannelActive();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("DCP Connection opened with Name \"{}\" against Node {}",
                    connectionName.toString(0, connectionName.writerIndex(), StandardCharsets.UTF_8),
                    ctx.channel().remoteAddress());
        }
    }

    private void helo(ChannelHandlerContext ctx, ByteBuf msg) {
        String response = MessageUtil.getContent(msg).toString(StandardCharsets.UTF_8);
        int majorVersion = 5;
        try {
            majorVersion = Integer.parseInt(response.substring(0, response.indexOf('.')));
        } catch (NumberFormatException e) {
            LOGGER.warn("Version returned by the server couldn't be parsed: {}", response, e);
        }
        if (majorVersion == 4) {
            step = OPEN;
            openConnection(ctx);
        } else {
            // Helo
            helo(ctx);
        }
    }

    private void helo(ChannelHandlerContext ctx) {
        ByteBuf request = ctx.alloc().buffer();
        Hello.init(request, connectionName);
        ctx.writeAndFlush(request);
    }

    private void openConnection(ChannelHandlerContext ctx) {
        ByteBuf request = ctx.alloc().buffer();
        DcpOpenConnectionRequest.init(request);
        connectionName.resetReaderIndex();
        DcpOpenConnectionRequest.connectionName(request, connectionName);
        ctx.writeAndFlush(request);
    }

    private String toString(byte step) {
        switch (step) {
            case VERSION:
                return "VERSION";
            case HELO:
                return "HELO";
            case SELECT:
                return "SELECT";
            case OPEN:
                return "OPEN";
            case REMOVE:
                return "REMOVE";
            default:
                return "UNKNOWN";
        }
    }
}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import java.nio.charset.StandardCharsets;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.config.ClientEnvironment;
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
     */
    DcpConnectHandler(final ClientEnvironment environment) {
        bucket = environment.bucket();
        connectionName = Unpooled.copiedBuffer(environment.connectionNameGenerator().name(), CharsetUtil.UTF_8);
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
            originalPromise().setFailure(
                    new IllegalStateException("Could not open DCP Connection: Failed in the " + toString(step)
                            + " step, response status is " + status + ": " + MemcachedStatus.toString(status)));
        }
    }

    private void remove(ChannelHandlerContext ctx) {
        ctx.pipeline().remove(this);
        originalPromise().setSuccess();
        ctx.fireChannelActive();
        LOGGER.debug("DCP Connection opened with Name \"{}\" against Node {}", connectionName,
                ctx.channel().remoteAddress());
    }

    private void helo(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        String response = MessageUtil.getContent(msg).toString(StandardCharsets.UTF_8);
        int majorVersion;
        try {
            majorVersion = Integer.parseInt(response.substring(0, 1));
        } catch (NumberFormatException e) {
            LOGGER.warn("Version returned by the server couldn't be parsed: " + response);
            throw new Exception("Version returned by the server couldn't be parsed: " + response, e);
        }
        if (majorVersion < 5) {
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

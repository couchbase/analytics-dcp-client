/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ConnectionNameGenerator;
import com.couchbase.client.dcp.message.DcpOpenConnectionRequest;
import com.couchbase.client.dcp.message.MessageUtil;
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
     * Status indicating a successful open connect attempt.
     */
    private static final byte CONNECT_SUCCESS = 0x00;

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpConnectHandler.class);

    /**
     * Generates the connection name for the dcp connection.
     */
    private final ConnectionNameGenerator connectionNameGenerator;

    /**
     * The generated connection name, set fresh once a channel becomes active.
     */
    private String connectionName;

    /**
     * Creates a new connect handler.
     *
     * @param connectionNameGenerator
     *            the generator of the connection names.
     */
    DcpConnectHandler(final ConnectionNameGenerator connectionNameGenerator) {
        this.connectionNameGenerator = connectionNameGenerator;
    }

    /**
     * Once the channel becomes active, sends the initial open connection request.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        connectionName = connectionNameGenerator.name();
        ByteBuf request = ctx.alloc().buffer();
        DcpOpenConnectionRequest.init(request);
        DcpOpenConnectionRequest.connectionName(request, Unpooled.copiedBuffer(connectionName, CharsetUtil.UTF_8));
        ctx.writeAndFlush(request);
    }

    /**
     * Once we get a response from the connect request, check if it is successful and complete/fail the connect
     * phase accordingly.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        short status = MessageUtil.getStatus(msg);
        if (status == CONNECT_SUCCESS) {
            ctx.pipeline().remove(this);
            originalPromise().setSuccess();
            ctx.fireChannelActive();
            LOGGER.debug("DCP Connection opened with Name \"{}\" against Node {}", connectionName,
                    ctx.channel().remoteAddress());
        } else {
            originalPromise()
                    .setFailure(new IllegalStateException("Could not open DCP Connection: Status is " + status));
        }
    }

}

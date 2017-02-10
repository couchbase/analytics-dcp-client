/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.transport.netty;

import java.util.Iterator;
import java.util.Map;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpControlRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

/**
 * Negotiates DCP control flags once connected and removes itself afterwards.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpNegotiationHandler extends ConnectInterceptingHandler<ByteBuf> {

    /**
     * Status indicating a successful negotiation of one control param.
     */
    private static final byte CONTROL_SUCCESS = 0x00;

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpNegotiationHandler.class);

    /**
     * Stores an iterator over the control settings that need to be negotiated.
     */
    private final Iterator<Map.Entry<String, String>> controlSettings;

    /**
     * Create a new dcp control handler.
     *
     * @param dcpControl
     *            the options set by the caller which should be negotiated.
     */
    DcpNegotiationHandler(final DcpControl dcpControl) {
        controlSettings = dcpControl.iterator();
    }

    /**
     * Helper method to walk the iterator and create a new request that defines which control param
     * should be negotiated right now.
     */
    private void negotiate(final ChannelHandlerContext ctx) {
        if (controlSettings.hasNext()) {
            Map.Entry<String, String> setting = controlSettings.next();

            LOGGER.debug("Negotiating DCP Control {}: {}", setting.getKey(), setting.getValue());
            ByteBuf request = ctx.alloc().buffer();
            DcpControlRequest.init(request);
            DcpControlRequest.key(Unpooled.copiedBuffer(setting.getKey(), CharsetUtil.UTF_8), request);
            DcpControlRequest.value(Unpooled.copiedBuffer(setting.getValue(), CharsetUtil.UTF_8), request);

            ctx.writeAndFlush(request);
        } else {
            originalPromise().setSuccess();
            ctx.pipeline().remove(this);
            ctx.fireChannelActive();
            LOGGER.debug("Negotiated all DCP Control settings against Node {}", ctx.channel().remoteAddress());
        }
    }

    /**
     * Once the channel becomes active, start negotiating the dcp control params.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        negotiate(ctx);
    }

    /**
     * Since only one feature per req/res can be negotiated, repeat the process once a response comes
     * back until the iterator is complete or a non-success response status is received.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        short status = MessageUtil.getStatus(msg);
        if (status == CONTROL_SUCCESS) {
            negotiate(ctx);
        } else {
            originalPromise().setFailure(new IllegalStateException("Could not configure DCP Controls: " + status));
        }
    }

}

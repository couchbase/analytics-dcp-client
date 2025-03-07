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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpControlRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.util.MemcachedStatus;

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
    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Stores an iterator over the control settings that need to be negotiated.
     */
    private final Iterator<Map.Entry<String, String>> controlSettings;

    private final LinkedHashMap<String, String> negotiatedSettings = new LinkedHashMap<>();

    private final LinkedHashMap<String, String> failedSettings = new LinkedHashMap<>();

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
            negotiatedSettings.put(setting.getKey(), setting.getValue());
            ByteBuf request = ctx.alloc().buffer();
            DcpControlRequest.init(request);
            DcpControlRequest.key(Unpooled.copiedBuffer(setting.getKey(), CharsetUtil.UTF_8), request);
            DcpControlRequest.value(Unpooled.copiedBuffer(setting.getValue(), CharsetUtil.UTF_8), request);

            ctx.writeAndFlush(request);
        } else {
            originalPromise().setSuccess();
            ctx.pipeline().remove(this);
            ctx.fireChannelActive();
            LOGGER.debug("negotiated {} (failed {}) against {}", negotiatedSettings, failedSettings,
                    ctx.channel().remoteAddress());
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
        if (status != CONTROL_SUCCESS) {
            Map.Entry<String, String> failed =
                    negotiatedSettings.entrySet().stream().reduce((first, second) -> second).orElseThrow();
            failedSettings.put(failed.getKey(), failed.getValue());
            negotiatedSettings.remove(failed.getKey());
            LOGGER.warn("Failed to configure DCP Control {}: {}", failed, MemcachedStatus.toString(status));
        }
        negotiate(ctx);
    }

    public Map<String, String> getNegotiatedSettings() {
        return Collections.unmodifiableMap(negotiatedSettings);
    }

    public Map<String, String> getFailedSettings() {
        return Collections.unmodifiableMap(failedSettings);
    }
}

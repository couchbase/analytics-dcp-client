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

import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollSocketChannel;
import com.couchbase.client.deps.io.netty.channel.oio.OioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.deps.io.netty.channel.socket.oio.OioSocketChannel;

/**
 * Various netty channel related utility methods.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public enum ChannelUtils {
    ;

    /**
     * Helper method to detect the right channel for the given event loop group.
     *
     * Supports Epoll, Nio and Oio.
     *
     * @param group
     *            the event loop group passed in.
     * @return returns the right channel class for the group.
     */
    public static Class<? extends Channel> channelForEventLoopGroup(final EventLoopGroup group) {
        Class<? extends Channel> channelClass = NioSocketChannel.class;
        if (group instanceof EpollEventLoopGroup) {
            channelClass = EpollSocketChannel.class;
        } else if (group instanceof OioEventLoopGroup) {
            channelClass = OioSocketChannel.class;
        }
        return channelClass;
    }
}

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

import java.io.IOException;
import java.util.Arrays;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;

import com.couchbase.client.core.endpoint.kv.AuthenticationException;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.security.sasl.Sasl;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.SaslAuthRequest;
import com.couchbase.client.dcp.message.SaslAuthResponse;
import com.couchbase.client.dcp.message.SaslListMechsRequest;
import com.couchbase.client.dcp.message.SaslListMechsResponse;
import com.couchbase.client.dcp.message.SaslStepRequest;
import com.couchbase.client.dcp.message.SaslStepResponse;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;

/**
 * Performs SASL authentication against the socket and once complete removes itself.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
class AuthHandler extends ConnectInterceptingHandler<ByteBuf> implements CallbackHandler {

    /**
     * Indicates a successful SASL auth.
     */
    private static final byte AUTH_SUCCESS = 0x00;

    /**
     * Indicates a failed SASL auth.
     */
    private static final byte AUTH_ERROR = 0x20;

    /**
     * The logger used for the auth handler.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(AuthHandler.class);

    /**
     * Username used to authenticate against the bucket (likely to be the bucket name itself).
     */
    private final String username;

    /**
     * The user/bucket password.
     */
    private final String password;

    /**
     * The SASL client reused from core-io since it has our SCRAM-* additions that are not
     * provided by the vanilla JDK implementation.
     */
    private SaslClient saslClient;

    /**
     * Stores the selected SASL mechanism in the process.
     */
    private String selectedMechanism;

    /**
     * Creates a new auth handler.
     *
     * @param username
     *            user/bucket name.
     * @param password
     *            password of the user/bucket.
     */
    AuthHandler(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * Once the channel is active, start the SASL auth negotiation.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        ByteBuf request = ctx.alloc().buffer();
        SaslListMechsRequest.init(request);
        ctx.writeAndFlush(request);
    }

    /**
     * Every time we recieve a message as part of the negotiation process, handle
     * it according to the req/res process.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        if (SaslListMechsResponse.is(msg)) {
            handleListMechsResponse(ctx, msg);
        } else if (SaslAuthResponse.is(msg)) {
            handleAuthResponse(ctx, msg);
        } else if (SaslStepResponse.is(msg)) {
            checkIsAuthed(ctx, MessageUtil.getStatus(msg));
        } else {
            throw new IllegalStateException("Received unexpected SASL response! " + MessageUtil.humanize(msg));
        }
    }

    /**
     * Runs the SASL challenge protocol and dispatches the next step if required.
     */
    private void handleAuthResponse(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        if (saslClient.isComplete()) {
            checkIsAuthed(ctx, MessageUtil.getStatus(msg));
            return;
        }

        ByteBuf challengeBuf = SaslAuthResponse.challenge(msg);
        byte[] challenge = new byte[challengeBuf.readableBytes()];
        challengeBuf.readBytes(challenge);
        byte[] evaluatedBytes = saslClient.evaluateChallenge(challenge);

        if (evaluatedBytes != null) {
            ByteBuf content;

            // This is needed against older server versions where the protocol does not
            // align on cram and plain, the else block is used for all the newer cram-sha*
            // mechanisms.
            //
            // Note that most likely this is only executed in the CRAM-MD5 case only, but
            // just to play it safe keep it for both mechanisms.
            if (selectedMechanism.equals("CRAM-MD5") || selectedMechanism.equals("PLAIN")) {
                String[] evaluated = new String(evaluatedBytes).split(" ");
                content = Unpooled.copiedBuffer(username + "\0" + evaluated[1], CharsetUtil.UTF_8);
            } else {
                content = Unpooled.wrappedBuffer(evaluatedBytes);
            }

            ByteBuf request = ctx.alloc().buffer();
            SaslStepRequest.init(request);
            SaslStepRequest.mechanism(Unpooled.copiedBuffer(selectedMechanism, CharsetUtil.UTF_8), request);
            SaslStepRequest.challengeResponse(content, request);

            ChannelFuture future = ctx.writeAndFlush(request);
            addFailureListener(future);
        } else {
            throw new AuthenticationException("SASL Challenge evaluation returned null.");
        }

    }

    private void addFailureListener(ChannelFuture future) {
        future.addListener((GenericFutureListener<Future<Void>>) f -> {
            if (!f.isSuccess()) {
                LOGGER.warn("Error during SASL Auth negotiation phase", f.cause());
                originalPromise().setFailure(f.cause());
            }
        });
    }

    /**
     * Check if the authenication process suceeded or failed based on the response status.
     */
    private void checkIsAuthed(final ChannelHandlerContext ctx, final short status) {
        switch (status) {
            case AUTH_SUCCESS:
                LOGGER.debug("Successfully authenticated against node {}", ctx.channel().remoteAddress());
                ctx.pipeline().remove(this);
                originalPromise().setSuccess();
                ctx.fireChannelActive();
                break;
            case AUTH_ERROR:
                originalPromise().setFailure(new AuthenticationException("SASL Authentication Failure"));
                break;
            default:
                originalPromise().setFailure(new AuthenticationException("Unhandled SASL auth status: " + status));
        }
    }

    /**
     * Helper method to parse the list of supported SASL mechs and dispatch the initial auth request following.
     */
    private void handleListMechsResponse(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        String remote = ctx.channel().remoteAddress().toString();
        String[] supportedMechanisms = SaslListMechsResponse.supportedMechs(msg);
        if (supportedMechanisms.length == 0) {
            throw new AuthenticationException("Received empty SASL mechanisms list from server: " + remote);
        }

        saslClient = Sasl.createSaslClient(supportedMechanisms, null, "couchbase", remote, null, this);
        if (saslClient == null) {
            throw new AuthenticationException("No supported SASL mechanisms from server " + remote
                    + " found; server sent: " + Arrays.toString(supportedMechanisms));
        }
        selectedMechanism = saslClient.getMechanismName();

        byte[] bytePayload = saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[] {}) : null;
        ByteBuf payload = bytePayload != null ? ctx.alloc().buffer().writeBytes(bytePayload) : Unpooled.EMPTY_BUFFER;

        ByteBuf request = ctx.alloc().buffer();
        SaslAuthRequest.init(request);
        SaslAuthRequest.mechanism(Unpooled.copiedBuffer(selectedMechanism, CharsetUtil.UTF_8), request);
        SaslAuthRequest.challengeResponse(payload, request);
        payload.release();

        ChannelFuture future = ctx.writeAndFlush(request);
        addFailureListener(future);
    }

    /**
     * Handles the SASL defined callbacks to set user and password (the {@link CallbackHandler} interface).
     */
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(username);
            } else if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(password.toCharArray());
            } else {
                throw new AuthenticationException("SASLClient requested unsupported callback: " + callback);
            }
        }
    }

}

/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.config;

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

import com.couchbase.client.core.endpoint.SSLException;

/**
 * Creates a {@link SSLEngine} which will be passed into the handler if SSL is enabled.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class SSLEngineFactory {

    /**
     * The global environment which is shared.
     */
    private final SecureEnvironment env;

    /**
     * Create a new engine factory.
     *
     * @param env
     *            the config environment.
     */
    public SSLEngineFactory(SecureEnvironment env) {
        this.env = env;
    }

    /**
     * Returns a new {@link SSLEngine} constructed from the config settings.
     *
     * @return a {@link SSLEngine} ready to be used.
     */
    public SSLEngine get() {
        try {
            String pass = env.sslKeystorePassword();
            char[] password = pass == null || pass.isEmpty() ? null : pass.toCharArray();

            KeyStore ks = env.sslKeystore();
            if (ks == null) {
                ks = KeyStore.getInstance(KeyStore.getDefaultType());
                String ksFile = env.sslKeystoreFile();
                if (ksFile == null || ksFile.isEmpty()) {
                    throw new IllegalArgumentException("Path to Keystore File must not be null or empty.");
                }
                ks.load(new FileInputStream(ksFile), password);
            }
            String defaultAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(defaultAlgorithm);
            tmf.init(ks);
            final KeyManager[] keyManagers;
            if (env.sslIncludeKeyMaterial()) {
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(defaultAlgorithm);
                kmf.init(ks, password);
                keyManagers = kmf.getKeyManagers();
            } else {
                keyManagers = null;
            }
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(keyManagers, tmf.getTrustManagers(), null);

            SSLEngine engine = ctx.createSSLEngine();
            engine.setUseClientMode(true);
            return engine;
        } catch (Exception ex) {
            throw new SSLException("Could not create SSLEngine.", ex);
        }
    }
}

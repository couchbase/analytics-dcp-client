/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.config;

import java.security.KeyStore;

public interface SecureEnvironment {
    /**
     * Identifies if SSL should be enabled.
     *
     * @return true if SSL is enabled, false otherwise.
     */
    boolean sslEnabled();

    /**
     * Identifies if SSL configuration should include the key material in the keystore file.
     *
     * @return true if the key material should be included, false otherwise.
     */
    boolean sslIncludeKeyMaterial();

    /**
     * Identifies the filepath to the ssl keystore.
     *
     * @return the path to the keystore file.
     */
    String sslKeystoreFile();

    /**
     * The password which is used to protect the keystore.
     *
     * @return the keystore password.
     */
    String sslKeystorePassword();

    /**
     * Allows to directly configure a {@link KeyStore}.
     *
     * @return the keystore to use.
     */
    KeyStore sslKeystore();
}

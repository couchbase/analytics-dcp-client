/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
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

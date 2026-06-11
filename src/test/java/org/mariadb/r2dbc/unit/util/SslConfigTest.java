// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.r2dbc.spi.R2dbcTransientResourceException;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.SslMode;
import org.mariadb.r2dbc.util.SslConfig;

/**
 * A missing {@code classpath:} serverSslCert must fail closed (or honor fallbackToSystemTrustStore)
 * rather than silently fall back to the JDK system trust store. {@code getResourceAsStream} returns
 * null for a missing resource, which Netty would otherwise turn into a system-trust-store context,
 * defeating certificate pinning.
 */
public class SslConfigTest {

  private static final String MISSING = "classpath:/does-not-exist-r2dbc-test-cert.pem";

  @Test
  void missingClasspathCertFailsClosedWithoutFallback() {
    // fallbackToSystemTrustStore = false -> must throw rather than use the system trust store
    assertThrows(
        R2dbcTransientResourceException.class,
        () ->
            new SslConfig(
                SslMode.VERIFY_CA, MISSING, null, null, null, null, false, null, false, false));
  }

  @Test
  void missingClasspathCertHonorsFallback() {
    // fallbackToSystemTrustStore = true -> allowed to fall back to the system trust store
    assertDoesNotThrow(
        () ->
            new SslConfig(
                SslMode.VERIFY_CA, MISSING, null, null, null, null, false, null, true, false));
  }
}

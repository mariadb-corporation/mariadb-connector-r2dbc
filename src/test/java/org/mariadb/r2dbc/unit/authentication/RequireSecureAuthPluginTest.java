// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.authentication;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.authentication.addon.ClearPasswordPluginFlow;
import org.mariadb.r2dbc.authentication.standard.CachingSha2PasswordFlow;
import org.mariadb.r2dbc.authentication.standard.Ed25519PasswordPluginFlow;
import org.mariadb.r2dbc.authentication.standard.NativePasswordPluginFlow;
import org.mariadb.r2dbc.authentication.standard.PamPluginFlow;
import org.mariadb.r2dbc.authentication.standard.ParsecPasswordPlugin;
import org.mariadb.r2dbc.authentication.standard.Sha256PasswordPluginFlow;

/**
 * R2DBC-118 (report by fg0x0): a clear-text password authentication plugin must declare {@link
 * org.mariadb.r2dbc.authentication.AuthenticationPlugin#requireSecure()} so the driver refuses to
 * run it over an insecure connection. This locks which plugins transmit the password in clear text.
 */
public class RequireSecureAuthPluginTest {

  @Test
  void clearTextPluginsRequireSecure() {
    assertTrue(new ClearPasswordPluginFlow().requireSecure(), "mysql_clear_password");
    assertTrue(new PamPluginFlow().requireSecure(), "dialog");
  }

  @Test
  void nonClearTextPluginsDoNotRequireSecure() {
    assertFalse(new NativePasswordPluginFlow().requireSecure(), "mysql_native_password");
    assertFalse(new Ed25519PasswordPluginFlow().requireSecure(), "client_ed25519");
    assertFalse(new Sha256PasswordPluginFlow().requireSecure(), "sha256_password");
    assertFalse(new CachingSha2PasswordFlow().requireSecure(), "caching_sha2_password");
    assertFalse(new ParsecPasswordPlugin().requireSecure(), "parsec");
  }
}

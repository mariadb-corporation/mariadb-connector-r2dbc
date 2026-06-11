// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication;

import io.r2dbc.spi.R2dbcException;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.server.Sequencer;

public interface AuthenticationPlugin {

  String type();

  AuthenticationPlugin create();

  /**
   * Whether this authentication plugin requires a secure connection (TLS or a local unix socket).
   * Plugins that transmit the password in clear text return {@code true}; the driver then refuses
   * to run them over a plain TCP connection, so a malicious server cannot harvest the password by
   * requesting a clear-text authentication plugin.
   *
   * @return true if a secure connection is required
   */
  default boolean requireSecure() {
    return false;
  }

  ClientMessage next(
      MariadbConnectionConfiguration configuration,
      byte[] seed,
      Sequencer sequencer,
      AuthMoreData authMoreData)
      throws R2dbcException;
}

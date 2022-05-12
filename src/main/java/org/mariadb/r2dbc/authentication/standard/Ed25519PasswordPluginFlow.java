// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication.standard;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.AuthSwitch;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.client.Ed25519PasswordPacket;

public final class Ed25519PasswordPluginFlow implements AuthenticationPlugin {

  public static final String TYPE = "client_ed25519";

  public Ed25519PasswordPluginFlow create() {
    return new Ed25519PasswordPluginFlow();
  }

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      AuthSwitch authSwitch,
      AuthMoreData authMoreData) {

    return new Ed25519PasswordPacket(
        authSwitch.getSequencer(), configuration.getPassword(), authSwitch.getSeed());
  }
}

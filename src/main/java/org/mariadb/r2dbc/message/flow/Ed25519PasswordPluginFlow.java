// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.flow;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.client.Ed25519PasswordPacket;
import org.mariadb.r2dbc.message.server.AuthMoreDataPacket;
import org.mariadb.r2dbc.message.server.AuthSwitchPacket;

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
      AuthSwitchPacket authSwitchPacket,
      AuthMoreDataPacket authMoreDataPacket) {

    return new Ed25519PasswordPacket(
        authSwitchPacket.getSequencer(), configuration.getPassword(), authSwitchPacket.getSeed());
  }
}

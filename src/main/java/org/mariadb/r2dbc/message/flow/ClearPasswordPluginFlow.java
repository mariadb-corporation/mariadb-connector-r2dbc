// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.flow;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.client.ClearPasswordPacket;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.server.AuthMoreDataPacket;
import org.mariadb.r2dbc.message.server.AuthSwitchPacket;

public final class ClearPasswordPluginFlow implements AuthenticationPlugin {

  public static final String TYPE = "mysql_clear_password";

  public ClearPasswordPluginFlow create() {
    return new ClearPasswordPluginFlow();
  }

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      AuthSwitchPacket authSwitchPacket,
      AuthMoreDataPacket authMoreDataPacket) {
    return new ClearPasswordPacket(authSwitchPacket.getSequencer(), configuration.getPassword());
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.flow;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.client.NativePasswordPacket;
import org.mariadb.r2dbc.message.server.AuthMoreDataPacket;
import org.mariadb.r2dbc.message.server.AuthSwitchPacket;

public final class NativePasswordPluginFlow implements AuthenticationPlugin {

  public static final String TYPE = "mysql_native_password";

  public NativePasswordPluginFlow create() {
    return new NativePasswordPluginFlow();
  }

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      AuthSwitchPacket authSwitchPacket,
      AuthMoreDataPacket authMoreDataPacket) {
    return new NativePasswordPacket(
        authSwitchPacket.getSequencer(), configuration.getPassword(), authSwitchPacket.getSeed());
  }
}

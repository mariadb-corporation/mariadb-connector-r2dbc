// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication.standard;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.AuthSwitch;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.client.NativePasswordPacket;

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
      AuthSwitch authSwitch,
      AuthMoreData authMoreData) {
    return new NativePasswordPacket(
        authSwitch.getSequencer(), configuration.getPassword(), authSwitch.getSeed());
  }
}

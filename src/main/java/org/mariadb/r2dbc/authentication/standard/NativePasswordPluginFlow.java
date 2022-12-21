// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication.standard;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.client.NativePasswordPacket;
import org.mariadb.r2dbc.message.server.Sequencer;

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
      byte[] seed,
      Sequencer sequencer,
      AuthMoreData authMoreData) {
    return new NativePasswordPacket(sequencer, configuration.getPassword(), seed);
  }
}

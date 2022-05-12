// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication.standard;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.AuthSwitch;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.client.ClearPasswordPacket;

public final class PamPluginFlow implements AuthenticationPlugin {

  public static final String TYPE = "dialog";

  public PamPluginFlow create() {
    return new PamPluginFlow();
  }

  private int counter = -1;

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      AuthSwitch authSwitch,
      AuthMoreData authMoreData) {
    while (true) {
      counter++;
      if (counter == 0) {
        return new ClearPasswordPacket(authSwitch.getSequencer(), configuration.getPassword());
      } else {
        if (configuration.getPamOtherPwd() == null) {
          throw new IllegalArgumentException(
              "PAM authentication is set with multiple password, but pamOtherPwd option wasn't set");
        }
        if (configuration.getPamOtherPwd().length < counter) {
          throw new IllegalArgumentException(
              String.format(
                  "PAM authentication required at least %s passwords, but pamOtherPwd has only %s",
                  counter, configuration.getPamOtherPwd().length));
        }
        return new ClearPasswordPacket(
            authSwitch.getSequencer(), configuration.getPamOtherPwd()[counter - 1]);
      }
    }
  }
}

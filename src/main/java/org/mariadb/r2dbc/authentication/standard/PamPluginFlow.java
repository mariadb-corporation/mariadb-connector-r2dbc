// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication.standard;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.client.ClearPasswordPacket;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class PamPluginFlow implements AuthenticationPlugin {

  public static final String TYPE = "dialog";
  private int counter = -1;

  public PamPluginFlow create() {
    return new PamPluginFlow();
  }

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      byte[] seed,
      Sequencer sequencer,
      AuthMoreData authMoreData) {
    while (true) {
      counter++;
      if (counter == 0) {
        return new ClearPasswordPacket(sequencer, configuration.getPassword());
      } else {
        if (configuration.getPamOtherPwd() == null) {
          throw new IllegalArgumentException(
              "PAM authentication is set with multiple password, but pamOtherPwd option wasn't"
                  + " set");
        }
        if (configuration.getPamOtherPwd().length < counter) {
          throw new IllegalArgumentException(
              String.format(
                  "PAM authentication required at least %s passwords, but pamOtherPwd has only %s",
                  counter, configuration.getPamOtherPwd().length));
        }
        return new ClearPasswordPacket(sequencer, configuration.getPamOtherPwd()[counter - 1]);
      }
    }
  }
}

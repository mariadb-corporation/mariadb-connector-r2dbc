/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.message.flow;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.client.ClearPasswordPacket;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.server.AuthMoreDataPacket;
import org.mariadb.r2dbc.message.server.AuthSwitchPacket;

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
      AuthSwitchPacket authSwitchPacket,
      AuthMoreDataPacket authMoreDataPacket) {
    while (true) {
      counter++;
      if (counter == 0) {
        return new ClearPasswordPacket(
            authSwitchPacket.getSequencer(), configuration.getPassword());
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
            authSwitchPacket.getSequencer(), configuration.getPamOtherPwd()[counter - 1]);
      }
    }
  }
}

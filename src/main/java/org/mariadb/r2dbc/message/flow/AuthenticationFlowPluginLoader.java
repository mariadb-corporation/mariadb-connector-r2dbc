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

import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;

import java.util.ServiceLoader;

public class AuthenticationFlowPluginLoader {

  private static ServiceLoader<AuthenticationPlugin> loader =
      ServiceLoader.load(AuthenticationPlugin.class, MariadbConnection.class.getClassLoader());

  /**
   * Get authentication plugin from type String. Customs authentication plugin can be added
   * implementing AuthenticationPlugin and registering new type in resources services.
   *
   * @param type authentication plugin type
   * @return Authentication plugin corresponding to type
   */
  public static AuthenticationPlugin get(String type) {
    if (type == null || type.isEmpty()) {
      return null;
    }

    for (AuthenticationPlugin implClass : loader) {
      if (type.equals(implClass.type())) {
        return implClass.create();
      }
    }

    throw new IllegalArgumentException(
        String.format(
            "Client does not support authentication protocol requested by server. "
                + "Plugin type was = '%s'",
            type));
  }
}

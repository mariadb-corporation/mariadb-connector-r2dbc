// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication;

import java.util.ServiceLoader;
import org.mariadb.r2dbc.api.MariadbConnection;

public class AuthenticationFlowPluginLoader {

  /**
   * Get authentication plugin from type String. Customs authentication plugin can be added
   * implementing AuthenticationPlugin and registering new type in resources services.
   *
   * @param type authentication plugin type
   * @return Authentication plugin corresponding to type
   */
  public static AuthenticationPlugin get(String type) {
    ServiceLoader<AuthenticationPlugin> loader =
        ServiceLoader.load(AuthenticationPlugin.class, MariadbConnection.class.getClassLoader());

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

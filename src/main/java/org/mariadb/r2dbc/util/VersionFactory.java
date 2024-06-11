// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class VersionFactory {

  public static class VersionFactoryHolder {
    public static final String instance;

    static {
      String res = null;
      try (InputStream inputStream =
          VersionFactory.class.getClassLoader().getResourceAsStream("mariadb.properties")) {
        if (inputStream != null) {
          Properties prop = new Properties();
          prop.load(inputStream);
          res = prop.getProperty("version");
        }
      } catch (IOException e) {
        // eat
      }
      instance = res;
    }
  }

  public static String getInstance() {
    return VersionFactoryHolder.instance;
  }
}

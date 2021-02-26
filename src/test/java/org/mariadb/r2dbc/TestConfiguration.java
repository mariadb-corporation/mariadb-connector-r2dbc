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

package org.mariadb.r2dbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestConfiguration {

  public static final String host;
  public static final int port;
  public static final String username;
  public static final String password;
  public static final String database;
  public static final String other;
  public static final MariadbConnectionConfiguration.Builder defaultBuilder;

  static {
    String defaultHost = "localhost";
    String defaultPort = "3306";
    String defaultDatabase = "testr2";
    String defaultPassword = "";
    String defaultUser = "root";
    String defaultOther = null;
    try (InputStream inputStream =
        BaseTest.class.getClassLoader().getResourceAsStream("conf.properties")) {
      Properties prop = new Properties();
      prop.load(inputStream);

      defaultHost = get("DB_HOST", prop);
      defaultPort = get("DB_PORT", prop);
      defaultDatabase = get("DB_DATABASE", prop);
      defaultPassword = get("DB_PASSWORD", prop);
      defaultUser = get("DB_USER", prop);
      defaultOther = get("DB_OTHER", prop);
    } catch (IOException io) {
      io.printStackTrace();
    }
    host = defaultHost;
    port = Integer.parseInt(defaultPort);
    database = defaultDatabase;
    password = defaultPassword;
    username = defaultUser;
    other = defaultOther;
    String connString = String.format(
        "r2dbc:mariadb://%s:%s@%s:%s/%s%s",
        username, password, host, port, database, other == null ? "" : "?" + other.replace("\n", "\\n"));
    System.out.println(connString);
    ConnectionFactoryOptions options = ConnectionFactoryOptions.parse(connString);
    System.out.println(options);
    defaultBuilder = MariadbConnectionConfiguration.fromOptions(options);
    System.out.println(defaultBuilder.build());
  }

  private static String get(String name, Properties prop) {
    String val = System.getenv("TEST_" + name);
    if (val == null) val = System.getProperty("TEST_" + name);
    if (val == null) val = prop.getProperty(name);
    return val;
  }

  public static final MariadbConnectionConfiguration defaultConf = defaultBuilder.build();
  public static final MariadbConnectionFactory defaultFactory =
      new MariadbConnectionFactory(defaultConf);
}

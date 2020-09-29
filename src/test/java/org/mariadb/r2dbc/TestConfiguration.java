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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestConfiguration {

  public static final String host;
  public static final int port;
  public static final String username;
  public static final String password;
  public static final String database;

  static {
    String defaultHost = "localhost";
    String defaultPort = "3306";
    String defaultDatabase = "testj";
    String defaultPassword = "";
    String defaultUser = "root";

    try (InputStream inputStream =
        BaseTest.class.getClassLoader().getResourceAsStream("conf.properties")) {
      Properties prop = new Properties();
      prop.load(inputStream);

      defaultHost = prop.getProperty("DB_HOST");
      defaultPort = prop.getProperty("DB_PORT");
      defaultDatabase = prop.getProperty("DB_DATABASE");
      defaultPassword = prop.getProperty("DB_PASSWORD");
      defaultUser = prop.getProperty("DB_USER");

    } catch (IOException io) {
      io.printStackTrace();
    }

    host = System.getProperty("TEST_HOST", defaultHost);
    port = Integer.parseInt(System.getProperty("TEST_PORT", defaultPort));
    database = System.getProperty("TEST_DATABASE", defaultDatabase);
    password = System.getProperty("TEST_PASSWORD", defaultPassword);
    username = System.getProperty("TEST_USERNAME", defaultUser);
  }

  public static final MariadbConnectionConfiguration.Builder defaultBuilder =
      MariadbConnectionConfiguration.builder()
          .host(host)
          .port(port)
          .username(username)
          .password(password)
          .database(database);

  public static final MariadbConnectionConfiguration defaultConf = defaultBuilder.build();
  public static final MariadbConnectionFactory defaultFactory =
      new MariadbConnectionFactory(defaultConf);
}

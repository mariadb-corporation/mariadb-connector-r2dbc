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

public class TestConfiguration {

  public static final String host = System.getProperty("TEST_HOST", "localhost");
  public static final int port = Integer.parseInt(System.getProperty("TEST_PORT", "3311"));
  public static final String username = System.getProperty("TEST_USERNAME", "root");
  public static final String password = System.getProperty("TEST_PASSWORD", "");
  public static final String database = System.getProperty("TEST_DATABASE", "testj");

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

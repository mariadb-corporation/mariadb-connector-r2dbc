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

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.ConnectionMetadata;

public interface MariadbConnectionMetadata extends ConnectionMetadata {

  @Override
  String getDatabaseProductName();

  @Override
  String getDatabaseVersion();

  /**
   * Short cut to indicate that database server is a MariaDB. i.e. equals to
   * "MariaDB".equals(getDatabaseProductName())
   *
   * @return true if database server is a MariaDB server.
   */
  boolean isMariaDBServer();

  /**
   * Indicate if server does have required version.
   *
   * @param major major version
   * @param minor minor version
   * @param patch patch version
   * @return true is database version is equal or more than indicated version
   */
  boolean minVersion(int major, int minor, int patch);

  /**
   * Indicate server major version.
   * in 10.5.4, return 10
   *
   * @return server major version
   */
  int getMajorVersion();

  /**
   * Indicate server minor version.
   * in 10.5.4, return 5
   *
   * @return server minor version
   */
  int getMinorVersion();

  /**
   * Indicate server patch version.
   * in 10.5.4, return 4
   *
   * @return server patch version
   */
  int getPatchVersion();

}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

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
   * Indicate server major version. in 10.5.4, return 10
   *
   * @return server major version
   */
  int getMajorVersion();

  /**
   * Indicate server minor version. in 10.5.4, return 5
   *
   * @return server minor version
   */
  int getMinorVersion();

  /**
   * Indicate server patch version. in 10.5.4, return 4
   *
   * @return server patch version
   */
  int getPatchVersion();
}

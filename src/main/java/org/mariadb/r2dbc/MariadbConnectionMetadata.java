// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import org.mariadb.r2dbc.client.ServerVersion;

public final class MariadbConnectionMetadata
    implements org.mariadb.r2dbc.api.MariadbConnectionMetadata {

  private final ServerVersion version;

  MariadbConnectionMetadata(ServerVersion version) {
    this.version = version;
  }

  @Override
  public String getDatabaseProductName() {
    return this.version.isMariaDBServer() ? "MariaDB" : "MySQL";
  }

  public boolean isMariaDBServer() {
    return this.version.isMariaDBServer();
  }

  public boolean minVersion(int major, int minor, int patch) {
    return this.version.versionGreaterOrEqual(major, minor, patch);
  }

  public int getMajorVersion() {
    return version.getMajorVersion();
  }

  public int getMinorVersion() {
    return version.getMinorVersion();
  }

  public int getPatchVersion() {
    return version.getPatchVersion();
  }

  @Override
  public String getDatabaseVersion() {
    return this.version.getServerVersion();
  }
}

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

  @Override
  public String getDatabaseVersion() {
    return this.version.getServerVersion();
  }
}

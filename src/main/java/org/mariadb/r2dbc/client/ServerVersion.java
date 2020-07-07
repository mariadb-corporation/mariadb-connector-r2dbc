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

package org.mariadb.r2dbc.client;

import java.util.Objects;

public class ServerVersion {

  public static final ServerVersion UNKNOWN_VERSION = new ServerVersion("0.0.0", true);
  private final String serverVersion;
  private final int majorVersion;
  private final int minorVersion;
  private final int patchVersion;
  private final boolean mariaDBServer;

  public ServerVersion(String serverVersion, boolean mariaDBServer) {
    this.serverVersion = serverVersion;
    this.mariaDBServer = mariaDBServer;
    int[] parsed = parseVersion(serverVersion);
    this.majorVersion = parsed[0];
    this.minorVersion = parsed[1];
    this.patchVersion = parsed[2];
  }

  public boolean isMariaDBServer() {
    return mariaDBServer;
  }

  public int getMajorVersion() {
    return majorVersion;
  }

  public int getMinorVersion() {
    return minorVersion;
  }

  public int getPatchVersion() {
    return patchVersion;
  }

  public String getServerVersion() {
    return serverVersion;
  }

  /**
   * Utility method to check if database version is greater than parameters.
   *
   * @param major major version
   * @param minor minor version
   * @param patch patch version
   * @return true if version is greater than parameters
   */
  public boolean versionGreaterOrEqual(int major, int minor, int patch) {
    if (this.majorVersion > major) {
      return true;
    }

    if (this.majorVersion < major) {
      return false;
    }

    /*
     * Major versions are equal, compare minor versions
     */
    if (this.minorVersion > minor) {
      return true;
    }
    if (this.minorVersion < minor) {
      return false;
    }

    // Minor versions are equal, compare patch version.
    return this.patchVersion >= patch;
  }

  private int[] parseVersion(String serverVersion) {
    int length = serverVersion.length();
    char car;
    int offset = 0;
    int type = 0;
    int val = 0;
    int majorVersion = 0;
    int minorVersion = 0;
    int patchVersion = 0;

    main_loop:
    for (; offset < length; offset++) {
      car = serverVersion.charAt(offset);
      if (car < '0' || car > '9') {
        switch (type) {
          case 0:
            majorVersion = val;
            break;
          case 1:
            minorVersion = val;
            break;
          case 2:
            patchVersion = val;
            break main_loop;
          default:
            break;
        }
        type++;
        val = 0;
      } else {
        val = val * 10 + car - 48;
      }
    }

    // serverVersion finished by number like "5.5.57", assign patchVersion
    if (type == 2) {
      patchVersion = val;
    }
    return new int[] {majorVersion, minorVersion, patchVersion};
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServerVersion that = (ServerVersion) o;
    return mariaDBServer == that.mariaDBServer && serverVersion.equals(that.serverVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serverVersion, mariaDBServer);
  }

  @Override
  public String toString() {
    return "ServerVersion{" + serverVersion + '}';
  }
}

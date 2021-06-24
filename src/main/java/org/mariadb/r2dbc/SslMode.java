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

public enum SslMode {

  // NO SSL
  DISABLE("disable", new String[] {"DISABLED", "0", "false"}),

  // Encryption only (no certificate and hostname validation) (DEVELOPMENT ONLY)
  TRUST("trust", new String[] {"REQUIRED", "ENABLE_TRUST"}),

  // Encryption, certificates validation, BUT no hostname verification
  VERIFY_CA("verify-ca", new String[] {"VERIFY_CA", "ENABLE_WITHOUT_HOSTNAME_VERIFICATION"}),

  // Standard SSL use: Encryption, certificate validation and hostname validation
  VERIFY_FULL("verify-full", new String[] {"VERIFY_IDENTITY", "1", "true", "enable"});

  private final String value;
  private final String[] aliases;

  SslMode(String value, String[] aliases) {
    this.value = value;
    this.aliases = aliases;
  }

  public static SslMode from(String value) {
    for (SslMode sslMode : values()) {
      if (sslMode.value.equalsIgnoreCase(value) || sslMode.name().equalsIgnoreCase(value)) {
        return sslMode;
      }
      for (String alias : sslMode.aliases) {
        if (alias.equalsIgnoreCase(value)) {
          return sslMode;
        }
      }
    }
    throw new IllegalArgumentException(
        String.format("Wrong argument value '%s' for SslMode", value));
  }
}

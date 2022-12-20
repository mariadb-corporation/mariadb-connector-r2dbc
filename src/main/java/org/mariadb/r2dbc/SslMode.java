// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

public enum SslMode {

  // NO SSL
  DISABLE("disable", new String[] {"DISABLED", "0", "false"}),

  // Encryption only (no certificate and hostname validation) (DEVELOPMENT ONLY)
  TRUST("trust", new String[] {"REQUIRED", "ENABLE_TRUST"}),

  // Encryption, certificates validation, BUT no hostname verification
  VERIFY_CA("verify-ca", new String[] {"VERIFY_CA", "ENABLE_WITHOUT_HOSTNAME_VERIFICATION"}),

  // Standard SSL use: Encryption, certificate validation and hostname validation
  VERIFY_FULL("verify-full", new String[] {"VERIFY_IDENTITY", "1", "true", "enable"}),

  // Connect over a pre-created SSL tunnel
  TUNNEL("tunnel", new String[]{"TUNNEL"});

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

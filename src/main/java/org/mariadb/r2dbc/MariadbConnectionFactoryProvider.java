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
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;
import org.mariadb.r2dbc.util.Assert;

import java.util.Map;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

public final class MariadbConnectionFactoryProvider implements ConnectionFactoryProvider {
  public static final String MARIADB_DRIVER = "mariadb";
  public static final Option<String> SOCKET = Option.valueOf("socket");
  public static final Option<Boolean> ALLOW_MULTI_QUERIES = Option.valueOf("allowMultiQueries");
  public static final Option<String> TLS_PROTOCOL = Option.valueOf("tlsProtocol");
  public static final Option<String> SERVER_SSL_CERT = Option.valueOf("serverSslCert");
  public static final Option<String> CLIENT_SSL_CERT = Option.valueOf("clientSslCert");
  public static final Option<String> SSL_MODE = Option.valueOf("sslMode");
  public static final Option<Map<String, String>> OPTIONS = Option.valueOf("options");

  static MariadbConnectionConfiguration createConfiguration(
      ConnectionFactoryOptions connectionFactoryOptions) {
    Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");
    return MariadbConnectionConfiguration.fromOptions(connectionFactoryOptions).build();
  }

  @Override
  public MariadbConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
    return new MariadbConnectionFactory(createConfiguration(connectionFactoryOptions));
  }

  @Override
  public String getDriver() {
    return MARIADB_DRIVER;
  }

  @Override
  public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
    Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

    String driver = connectionFactoryOptions.getValue(DRIVER);
    return driver != null && driver.equals(MARIADB_DRIVER);
  }
}

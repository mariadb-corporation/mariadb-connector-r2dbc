// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class TestConfiguration {

  public static final String host;
  public static final int port;
  public static final String username;
  public static final String password;
  public static final String database;
  public static final String other;
  public static final MariadbConnectionConfiguration.Builder defaultBuilder;
  public static final MariadbConnectionConfiguration defaultConf;
  public static final MariadbConnectionFactory defaultFactory;

  static {
    String defaultHost = "localhost";
    String defaultPort = "3306";
    String defaultDatabase = "testr2";
    String defaultPassword = "";
    String defaultUser = "root";
    String defaultOther = null;

    try (InputStream inputStream =
        BaseConnectionTest.class.getClassLoader().getResourceAsStream("conf.properties")) {
      Properties prop = new Properties();
      prop.load(inputStream);

      defaultHost = get("DB_HOST", prop);
      defaultPort = get("DB_PORT", prop);
      defaultDatabase = get("DB_DATABASE", prop);
      defaultPassword = get("DB_PASSWORD", prop);
      defaultUser = get("DB_USER", prop);

      String val = System.getenv("TEST_REQUIRE_TLS");
      if ("1".equals(val)) {
        String cert = System.getenv("TEST_DB_SERVER_CERT");
        defaultOther = "sslMode=enable&serverSslCert=" + cert;
      } else {
        defaultOther = get("DB_OTHER", prop);
      }
    } catch (IOException io) {
      io.printStackTrace();
    }
    host = defaultHost;
    port = Integer.parseInt(defaultPort);
    database = defaultDatabase;
    password = defaultPassword;
    username = defaultUser;
    other = defaultOther;
    String encodedUser;
    String encodedPwd;
    try {
      encodedUser = URLEncoder.encode(username, StandardCharsets.UTF_8.toString());
      encodedPwd = URLEncoder.encode(password, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException e) {
      encodedUser = username;
      encodedPwd = password;
    }
    String connString =
        String.format(
            "r2dbc:mariadb://%s:%s@%s:%s/%s%s",
            encodedUser,
            encodedPwd,
            host,
            port,
            database,
            other == null ? "" : "?" + other.replace("\n", "\\n"));

    ConnectionFactoryOptions options = ConnectionFactoryOptions.parse(connString);
    defaultBuilder = MariadbConnectionConfiguration.fromOptions(options);
    try {
      MariadbConnection connection = new MariadbConnectionFactory(MariadbConnectionConfiguration.fromOptions(options).allowPublicKeyRetrieval(true).build()).create().block();
      MariadbConnectionMetadata meta = connection.getMetadata();
      if (!meta.isMariaDBServer() && meta.minVersion(8,4,0)) {
        defaultBuilder.allowPublicKeyRetrieval(true);
      }
      connection.close().block();
    } catch (Exception e) {
      // eat
      e.printStackTrace();
    }

    defaultConf = defaultBuilder.build();
    defaultFactory = new MariadbConnectionFactory(defaultConf);
  }

  private static String get(String name, Properties prop) {
    String val = System.getenv("TEST_" + name);
    if (val == null) val = System.getProperty("TEST_" + name);
    if (val == null) val = prop.getProperty(name);
    return val;
  }
}

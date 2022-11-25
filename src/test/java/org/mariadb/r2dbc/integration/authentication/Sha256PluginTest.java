// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.authentication;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.io.File;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class Sha256PluginTest extends BaseConnectionTest {

  private static String rsaPublicKey;
  private static String cachingRsaPublicKey;
  private static final boolean isWindows =
      System.getProperty("os.name").toLowerCase().contains("win");

  private static boolean validPath(String path) {
    if (path == null) return false;
    try {
      File f = new File(path);
      return f.exists();
    } catch (Exception e) {
      // eat
    }
    return false;
  }

  @BeforeAll
  public static void init() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(5, 7, 0));

    rsaPublicKey = System.getProperty("rsaPublicKey");
    if (!validPath(rsaPublicKey) && minVersion(5, 7, 0)) {
      rsaPublicKey = System.getenv("TEST_DB_RSA_PUBLIC_KEY");
      if (!validPath(rsaPublicKey)) {
        try {
          rsaPublicKey =
              sharedConn
                  .createStatement("SELECT @@sha256_password_public_key_path")
                  .execute()
                  .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
                  .blockLast();
        } catch (R2dbcTransientResourceException e) {
          // eat
        }
        if (!validPath(rsaPublicKey)) {
          File sslDir = new File(System.getProperty("user.dir") + "/ssl");
          if (sslDir.exists() && sslDir.isDirectory()) {
            rsaPublicKey = System.getProperty("user.dir") + "/ssl/public.key";
          } else rsaPublicKey = null;
        }
      }

      cachingRsaPublicKey = System.getProperty("cachingRsaPublicKey");
      if (!validPath(cachingRsaPublicKey)) {
        cachingRsaPublicKey = System.getenv("TEST_DB_RSA_PUBLIC_KEY");
        if (!validPath(cachingRsaPublicKey)) {
          try {
            cachingRsaPublicKey =
                sharedConn
                    .createStatement("SELECT @@caching_sha2_password_public_key_path")
                    .execute()
                    .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
                    .blockLast();
          } catch (R2dbcTransientResourceException e) {
            // eat
          }
          if (!validPath(cachingRsaPublicKey)) {
            File sslDir = new File(System.getProperty("user.dir") + "/ssl");
            if (sslDir.exists() && sslDir.isDirectory()) {
              cachingRsaPublicKey = System.getProperty("user.dir") + "/ssl/public.key";
            } else cachingRsaPublicKey = null;
          }
        }
      }
      dropAll();

      String sqlCreateUser;
      String sqlGrant;
      String sqlCreateUser2;
      String sqlGrant2;
      String sqlCreateUser3;
      String sqlGrant3;

      if (minVersion(8, 0, 0)) {
        sqlCreateUser = "CREATE USER 'sha256User' IDENTIFIED WITH sha256_password BY 'password'";
        sqlGrant = "GRANT ALL PRIVILEGES ON *.* TO 'sha256User'";
        sqlCreateUser2 = "CREATE USER 'sha256User2' IDENTIFIED WITH sha256_password BY 'password'";
        sqlGrant2 = "GRANT ALL PRIVILEGES ON *.* TO 'sha256User2'";
        sqlCreateUser3 = "CREATE USER 'sha256User3' IDENTIFIED WITH sha256_password BY ''";
        sqlGrant3 = "GRANT ALL PRIVILEGES ON *.* TO 'sha256User3'";
      } else {
        sqlCreateUser = "CREATE USER 'sha256User'";
        sqlGrant =
            "GRANT ALL PRIVILEGES ON *.* TO 'sha256User' IDENTIFIED WITH "
                + "sha256_password BY 'password'";
        sqlCreateUser2 = "CREATE USER 'sha256User2'";
        sqlGrant2 =
            "GRANT ALL PRIVILEGES ON *.* TO 'sha256User2' IDENTIFIED WITH "
                + "sha256_password BY 'password'";
        sqlCreateUser3 = "CREATE USER 'sha256User3'";
        sqlGrant3 =
            "GRANT ALL PRIVILEGES ON *.* TO 'sha256User3' IDENTIFIED WITH "
                + "sha256_password BY ''";
      }
      sharedConn.createStatement(sqlCreateUser).execute().blockLast();
      sharedConn.createStatement(sqlGrant).execute().blockLast();
      sharedConn.createStatement(sqlCreateUser2).execute().blockLast();
      sharedConn.createStatement(sqlGrant2).execute().blockLast();
      sharedConn.createStatement(sqlCreateUser3).execute().blockLast();
      sharedConn.createStatement(sqlGrant3).execute().blockLast();
      if (minVersion(8, 0, 0)) {
        sharedConn
            .createStatement(
                "CREATE USER 'cachingSha256User'  IDENTIFIED WITH caching_sha2_password BY 'password'")
            .execute()
            .blockLast();
        sharedConn
            .createStatement("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User'")
            .execute()
            .blockLast();
        sharedConn
            .createStatement(
                "CREATE USER 'cachingSha256User2'  IDENTIFIED WITH caching_sha2_password BY 'password'")
            .execute()
            .blockLast();
        sharedConn
            .createStatement("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User2'")
            .execute()
            .blockLast();
        sharedConn
            .createStatement(
                "CREATE USER 'cachingSha256User3'  IDENTIFIED WITH caching_sha2_password BY 'password'")
            .execute()
            .blockLast();
        sharedConn
            .createStatement("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User3'")
            .execute()
            .blockLast();
        sharedConn
            .createStatement(
                "CREATE USER 'cachingSha256User4'  IDENTIFIED WITH caching_sha2_password BY ''")
            .execute()
            .blockLast();
        sharedConn
            .createStatement("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User4'")
            .execute()
            .blockLast();
      }
    }
  }

  @AfterAll
  public static void dropAll() {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(5, 7, 0));
    sharedConn
        .createStatement("DROP USER sha256User")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();
    sharedConn
        .createStatement("DROP USER sha256User2")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();
    sharedConn
        .createStatement("DROP USER sha256User3")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();
    sharedConn
        .createStatement("DROP USER cachingSha256User")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();
    sharedConn
        .createStatement("DROP USER cachingSha256User2")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();
    sharedConn
        .createStatement("DROP USER cachingSha256User3")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();
    sharedConn
        .createStatement("DROP USER cachingSha256User4")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();
  }

  @Test
  public void sha256PluginTestWithServerRsaKey() throws Exception {
    Assumptions.assumeTrue(
        !isWindows && !isMariaDBServer() && rsaPublicKey != null && minVersion(5, 7, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("sha256User")
            .password("password")
            .rsaPublicKey(rsaPublicKey)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }

  @Test
  public void sha256PluginTestWrongServerRsaKey() throws Exception {
    Assumptions.assumeTrue(!isWindows && !isMariaDBServer() && minVersion(5, 7, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("sha256User")
            .password("password")
            .rsaPublicKey("/wrongPath")
            .build();
    assertThrows(
        Exception.class,
        () -> new MariadbConnectionFactory(conf).create().block(),
        "Could not read server RSA public key from file");
  }

  @Test
  public void sha256PluginTestWithoutServerRsaKey() throws Exception {
    Assumptions.assumeTrue(
        !isWindows && !isMariaDBServer() && (minVersion(8, 0, 0) && !minVersion(8, 0, 31)));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("sha256User2")
            .password("password")
            .allowPublicKeyRetrieval(true)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }

  @Test
  public void sha256PluginTestException() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(8, 0, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("sha256User")
            .password("password")
            .build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .contains("RSA public key is not available client side"))
        .verify();
  }

  @Test
  public void sha256PluginTestSsl() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("sha256User")
            .password("password")
            .allowPublicKeyRetrieval(true)
            .sslMode(SslMode.TRUST)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }

  @Test
  public void sha256PluginTestSslNoPwd() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("sha256User3")
            .password(null)
            .sslMode(SslMode.TRUST)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }

  @Test
  public void cachingSha256PluginTestWithServerRsaKey() throws Exception {
    Assumptions.assumeTrue(
        !isWindows && !isMariaDBServer() && cachingRsaPublicKey != null && minVersion(8, 0, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("cachingSha256User")
            .password("password")
            .cachingRsaPublicKey(cachingRsaPublicKey)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }

  @Test
  public void cachingSha256PluginTestWithoutServerRsaKey() throws Exception {
    Assumptions.assumeTrue(!isWindows && rsaPublicKey != null && minVersion(8, 0, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("cachingSha256User2")
            .password("password")
            .allowPublicKeyRetrieval(true)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }

  @Test
  public void cachingSha256PluginTestException() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(8, 0, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("cachingSha256User3")
            .password("password")
            .build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable -> {
              throwable.printStackTrace();
              return throwable instanceof R2dbcNonTransientResourceException
                  && throwable.getMessage().contains("RSA public key is not available client side");
            })
        .verify();
  }

  @Test
  public void cachingSha256PluginTestSsl() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(8, 0, 0));
    Assumptions.assumeTrue(haveSsl(sharedConn));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("cachingSha256User")
            .password("password")
            .sslMode(SslMode.TRUST)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
    MariadbConnection connection3 = new MariadbConnectionFactory(conf).create().block();
    connection3.close().block();
  }

  @Test
  public void cachingSha256PluginTestNoPwd() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(8, 0, 0));
    Assumptions.assumeTrue(haveSsl(sharedConn));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("cachingSha256User4")
            .password(null)
            .sslMode(SslMode.TRUST)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
    MariadbConnection connection3 = new MariadbConnectionFactory(conf).create().block();
    connection3.close().block();
  }
}

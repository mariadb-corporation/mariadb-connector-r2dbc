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

package org.mariadb.r2dbc.integration.authentication;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class Sha256PluginTest extends BaseTest {

  private static String rsaPublicKey;
  private static String cachingRsaPublicKey;

  @BeforeAll
  public static void init() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(5, 7, 0));

    rsaPublicKey = System.getProperty("rsaPublicKey");
    if (rsaPublicKey == null) {
      rsaPublicKey =
          sharedConn
              .createStatement("SELECT @@caching_sha2_password_public_key_path")
              .execute()
              .map(res -> res.map((row, meta) -> row.get(0, String.class)))
              .singleOrEmpty()
              .block()
              .singleOrEmpty()
              .block();
    }

    cachingRsaPublicKey = System.getProperty("cachingRsaPublicKey");
    if (cachingRsaPublicKey == null) {
      cachingRsaPublicKey =
          sharedConn
              .createStatement("SELECT @@sha256_password_public_key_path")
              .execute()
              .map(res -> res.map((row, meta) -> row.get(0, String.class)))
              .singleOrEmpty()
              .block()
              .singleOrEmpty()
              .block();
    }

    sharedConn
        .createStatement("DROP USER 'sha256User'@'%'")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .subscribe();
    sharedConn
        .createStatement("DROP USER 'cachingSha256User'@'%'")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .subscribe();
    sharedConn
        .createStatement("DROP USER 'cachingSha256User2'@'%'")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();

    String sqlCreateUser;
    String sqlGrant;
    if (minVersion(8, 0, 0)) {
      sqlCreateUser = "CREATE USER 'sha256User'@'%' IDENTIFIED WITH sha256_password BY 'password'";
      sqlGrant = "GRANT ALL PRIVILEGES ON *.* TO 'sha256User'@'%'";
    } else {
      sqlCreateUser = "CREATE USER 'sha256User'@'%'";
      sqlGrant =
          "GRANT ALL PRIVILEGES ON *.* TO 'sha256User'@'%' IDENTIFIED WITH "
              + "sha256_password BY 'password'";
    }
    sharedConn.createStatement(sqlCreateUser).execute().subscribe();
    sharedConn.createStatement(sqlGrant).execute().blockLast();
    if (minVersion(8, 0, 0)) {
      sharedConn
          .createStatement(
              "CREATE USER 'cachingSha256User'@'%'  IDENTIFIED WITH caching_sha2_password BY 'password'")
          .execute()
          .subscribe();
      sharedConn
          .createStatement("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User'@'%'")
          .execute()
          .blockLast();
      sharedConn
          .createStatement(
              "CREATE USER 'cachingSha256User2'@'%'  IDENTIFIED WITH caching_sha2_password BY 'password'")
          .execute()
          .subscribe();
      sharedConn
          .createStatement("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User2'@'%'")
          .execute()
          .blockLast();
      sharedConn
          .createStatement(
              "CREATE USER 'cachingSha256User3'@'%'  IDENTIFIED WITH caching_sha2_password BY 'password'")
          .execute()
          .subscribe();
      sharedConn
          .createStatement("GRANT ALL PRIVILEGES ON *.* TO 'cachingSha256User3'@'%'")
          .execute()
          .blockLast();
    }
  }

  @AfterAll
  public static void after2() {
    sharedConn
        .createStatement("DROP USER sha256User")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .subscribe();
    sharedConn
        .createStatement("DROP USER cachingSha256User")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .subscribe();
    sharedConn
        .createStatement("DROP USER cachingSha256User2")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .subscribe();
    sharedConn
        .createStatement("DROP USER cachingSha256User3")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .subscribe();
  }

  @Test
  public void sha256PluginTestWithServerRsaKey() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer());
    Assumptions.assumeTrue(rsaPublicKey != null);
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    Assumptions.assumeTrue(meta.minVersion(8, 0, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("sha256User")
            .password("password")
            .rsaPublicKey(rsaPublicKey)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close();
  }

  @Test
  public void sha256PluginTestWithoutServerRsaKey() throws Exception {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    Assumptions.assumeTrue(meta.minVersion(8, 0, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("sha256User")
            .password("password")
            .allowPublicKeyRetrieval(true)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close();
  }

  @Test
  public void sha256PluginTestException() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer());
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    Assumptions.assumeTrue(meta.minVersion(8, 0, 0));

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
            .sslMode(SslMode.ENABLE_TRUST)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close();
  }

  @Test
  public void cachingSha256PluginTestWithServerRsaKey() throws Exception {
    Assumptions.assumeTrue(!isMariaDBServer());
    Assumptions.assumeTrue(cachingRsaPublicKey != null);
    Assumptions.assumeTrue(minVersion(8, 0, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("cachingSha256User")
            .password("password")
            .cachingRsaPublicKey(cachingRsaPublicKey)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close();
  }

  @Test
  public void cachingSha256PluginTestWithoutServerRsaKey() throws Exception {
    Assumptions.assumeTrue(rsaPublicKey != null);
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    Assumptions.assumeTrue(meta.minVersion(8, 0, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("cachingSha256User2")
            .password("password")
            .allowPublicKeyRetrieval(true)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close();

    MariadbConnectionConfiguration conf2 =
        TestConfiguration.defaultBuilder
            .clone()
            .username("cachingSha256User")
            .password("password")
            .build();
    MariadbConnection connection2 = new MariadbConnectionFactory(conf2).create().block();
    connection2.close();
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
            .sslMode(SslMode.ENABLE_TRUST)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close();
    MariadbConnection connection3 = new MariadbConnectionFactory(conf).create().block();
    connection3.close();
  }
}

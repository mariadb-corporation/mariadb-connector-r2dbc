// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.authentication;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class Ed25519PluginTest extends BaseConnectionTest {
  static AtomicBoolean ed25519PluginEnabled = new AtomicBoolean(true);

  @BeforeAll
  public static void before2() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    if (meta.isMariaDBServer() && meta.minVersion(10, 2, 0)) {
      sharedConn.createStatement("INSTALL SONAME 'auth_ed25519'").execute().blockLast();
      if (meta.minVersion(10, 4, 0)) {
        sharedConn
            .createStatement(
                "CREATE USER verificationEd25519AuthPlugin"+ getHostSuffix()+" IDENTIFIED "
                    + "VIA ed25519 USING PASSWORD('MySup8%rPassw@ord')")
            .execute()
            .flatMap(it -> it.getRowsUpdated())
            .onErrorResume(
                e -> {
                  ed25519PluginEnabled.set(false);
                  return Flux.just(1L);
                })
            .blockLast();

      } else {
        sharedConn
            .createStatement(
                "CREATE USER verificationEd25519AuthPlugin"+ getHostSuffix()+" IDENTIFIED "
                    + "VIA ed25519 USING '6aW9C7ENlasUfymtfMvMZZtnkCVlcb1ssxOLJ0kj/AA'")
            .execute()
            .flatMap(it -> it.getRowsUpdated())
            .onErrorResume(
                e -> {
                  ed25519PluginEnabled.set(false);
                  return Flux.just(1L);
                })
            .blockLast();
      }
      sharedConn
          .createStatement(
              String.format(
                  "GRANT SELECT on `%s`.* to verificationEd25519AuthPlugin",
                  TestConfiguration.database)+ getHostSuffix())
          .execute()
          .flatMap(it -> it.getRowsUpdated())
          .onErrorResume(
              e -> {
                ed25519PluginEnabled.set(false);
                return Flux.just(1L);
              })
          .blockLast();
      sharedConn.createStatement("FLUSH PRIVILEGES").execute().blockLast();
    }
  }

  @AfterAll
  public static void after2() {
    sharedConn
        .createStatement("DROP USER IF EXISTS verificationEd25519AuthPlugin" + getHostSuffix())
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();
  }

  @Test
  public void verificationEd25519AuthPlugin() throws Throwable {
    Assumptions.assumeTrue(
        ed25519PluginEnabled.get() && !isMaxscale() && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    Assumptions.assumeTrue(meta.isMariaDBServer() && meta.minVersion(10, 2, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("verificationEd25519AuthPlugin")
            .password("MySup8%rPassw@ord")
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }

  @Test
  public void verificationEd25519AuthPluginRestricted() throws Throwable {
    Assumptions.assumeTrue(
        ed25519PluginEnabled.get() && !isMaxscale() && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    Assumptions.assumeTrue(meta.isMariaDBServer() && meta.minVersion(10, 2, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("verificationEd25519AuthPlugin")
            .password("MySup8%rPassw@ord")
            .restrictedAuth("mysql_native_password")
            .sslMode(
                SslMode.from("1".equals(System.getenv("TEST_REQUIRE_TLS")) ? "trust" : "disabled"))
            .build();
    assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> new MariadbConnectionFactory(conf).create().block(),
        "Unsupported authentication plugin client_ed25519. Authorized plugin:"
            + " [mysql_native_password]");
  }

  @Test
  public void multiAuthPlugin() throws Throwable {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && System.getenv("TEST_PAM_USER") != null);
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 4, 2));
    sharedConn.createStatement("INSTALL PLUGIN pam SONAME 'auth_pam'").execute().blockLast();
    sharedConn.createStatement("drop user IF EXISTS mysqltest1").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE USER mysqltest1 IDENTIFIED "
                + "VIA pam "
                + " OR ed25519 as password('!Passw0rd3')"
                + " OR mysql_native_password as password('!Passw0rd3Works')")
        .execute()
        .blockLast();

    sharedConn.createStatement("GRANT SELECT on *.* to mysqltest1").execute().blockLast();
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("mysqltest1")
            .password("!Passw0rd3")
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();

    conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("mysqltest1")
            .password("!Passw0rd3Works")
            .build();
    connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
    sharedConn.createStatement("drop user mysqltest1@'%'").execute().blockLast();
  }

  @Test
  public void multiAuthPluginRestricted() throws Throwable {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && System.getenv("TEST_PAM_USER") != null);
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 4, 2));
    sharedConn.createStatement("INSTALL PLUGIN pam SONAME 'auth_pam'").execute().blockLast();
    sharedConn.createStatement("drop user IF EXISTS mysqltest1").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE USER mysqltest1 IDENTIFIED "
                + "VIA pam "
                + " OR ed25519 as password('!Passw0rd3')"
                + " OR mysql_native_password as password('!Passw0rd3Works')")
        .execute()
        .blockLast();

    sharedConn.createStatement("GRANT SELECT on *.* to mysqltest1").execute().blockLast();
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("mysqltest1")
            .password("!Passw0rd3")
            .restrictedAuth("mysql_native_password,dialog,mysql_clear_password")
            .build();
    assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> new MariadbConnectionFactory(conf).create().block(),
        "Unsupported authentication plugin client_ed25519. Authorized plugin:"
            + " [mysql_native_password, dialog, mysql_clear_password]");

    MariadbConnectionConfiguration conf2 =
        TestConfiguration.defaultBuilder
            .clone()
            .username("mysqltest1")
            .restrictedAuth("mysql_native_password,ed25519")
            .build();
    assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> new MariadbConnectionFactory(conf2).create().block(),
        "Unsupported authentication plugin");
  }
}

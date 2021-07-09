// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.authentication;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.core.publisher.Flux;

public class Ed25519PluginTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    if (meta.isMariaDBServer() && meta.minVersion(10, 2, 0)) {
      sharedConn
          .createStatement("INSTALL SONAME 'auth_ed25519'")
          .execute()
          .map(res -> res.getRowsUpdated())
          .onErrorReturn(Flux.empty())
          .blockLast();
      if (meta.minVersion(10, 4, 0)) {
        sharedConn
            .createStatement(
                "CREATE USER verificationEd25519AuthPlugin IDENTIFIED "
                    + "VIA ed25519 USING PASSWORD('MySup8%rPassw@ord')")
            .execute()
            .blockLast();
      } else {
        sharedConn
            .createStatement(
                "CREATE USER verificationEd25519AuthPlugin IDENTIFIED "
                    + "VIA ed25519 USING '6aW9C7ENlasUfymtfMvMZZtnkCVlcb1ssxOLJ0kj/AA'")
            .execute()
            .blockLast();
      }
      sharedConn
          .createStatement(
              String.format(
                  "GRANT SELECT on `%s`.* to verificationEd25519AuthPlugin",
                  TestConfiguration.database))
          .execute()
          .blockLast();
      sharedConn.createStatement("FLUSH PRIVILEGES").execute().blockLast();
    }
  }

  @AfterAll
  public static void after2() {
    sharedConn
        .createStatement("DROP USER IF EXISTS verificationEd25519AuthPlugin")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Flux.empty())
        .blockLast();
  }

  @Test
  public void verificationEd25519AuthPlugin() throws Throwable {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    Assumptions.assumeTrue(meta.isMariaDBServer() && meta.minVersion(10, 2, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("verificationEd25519AuthPlugin")
            .password("MySup8%rPassw@ord")
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close();
  }

  @Test
  public void multiAuthPlugin() throws Throwable {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 4, 2));

    sharedConn.createStatement("drop user IF EXISTS mysqltest1").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE USER mysqltest1 IDENTIFIED "
                + "VIA ed25519 as password('!Passw0rd3') "
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
}

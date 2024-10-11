// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.authentication;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.core.publisher.Flux;

public class ParsecPluginTest extends BaseConnectionTest {

  static AtomicBoolean parsecPluginEnabled = new AtomicBoolean(true);

  @BeforeAll
  public static void before2() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    if (meta.isMariaDBServer()
        && meta.minVersion(11, 6, 1)
        && !"mariadb-es".equals(System.getenv("srv"))) {
      sharedConn
          .createStatement("INSTALL SONAME 'auth_parsec'")
          .execute()
          .onErrorResume(
              e -> {
                parsecPluginEnabled.set(false);
                return Flux.empty();
              })
          .blockLast();
    } else parsecPluginEnabled.set(false);
  }

  @Test
  public void parsecAuthPlugin() throws Throwable {
    Assumptions.assumeTrue(
        parsecPluginEnabled.get()
            && !"maxscale".equals(System.getenv("srv"))
            && !"mariadb-es".equals(System.getenv("srv")));

    sharedConn.createStatement("drop user IF EXISTS verifParsec@'%'").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE USER verifParsec@'%' IDENTIFIED VIA parsec USING PASSWORD('MySup8%rPassw@ord')")
        .execute()
        .blockLast();
    sharedConn.createStatement("GRANT SELECT on *.* to verifParsec@'%'").execute().blockLast();

    String version = System.getProperty("java.version");
    int majorVersion =
        (version.indexOf(".") >= 0)
            ? Integer.parseInt(version.substring(0, version.indexOf(".")))
            : Integer.parseInt(version);
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("verifParsec")
            .password("MySup8%rPassw@ord")
            .build();

    if (majorVersion < 15) {
      // before java 15, Ed25519 is not supported
      // assuming, that BouncyCastle is not on test classpath
      MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();

      assertThrowsContains(
          SQLException.class,
          () -> new MariadbConnectionFactory(conf).create().block(),
          "Parsec authentication not available. Either use Java 15+ or add BouncyCastle"
              + " dependency");
    } else {
      MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
      connection.close().block();
    }
    MariadbConnectionConfiguration conf2 =
        TestConfiguration.defaultBuilder
            .clone()
            .username("verifParsec")
            .password("MySup8%rPassw@ord")
            .restrictedAuth("mysql_native_password,ed25519")
            .build();
    assertThrowsContains(
        R2dbcNonTransientResourceException.class,
        () -> new MariadbConnectionFactory(conf2).create().block(),
        "Unsupported authentication plugin parsec");
    sharedConn.createStatement("drop user verifParsec@'%'").execute().blockLast();
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.authentication;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;

public class PamPluginTest extends BaseConnectionTest {

  @Test
  public void pamAuthPlugin() throws Throwable {
    // https://mariadb.com/kb/en/authentication-plugin-pam/
    // only test on travis, because only work on Unix-like operating systems.
    // /etc/pam.d/mariadb pam configuration is created beforehand
    Assumptions.assumeTrue(
        System.getenv("TRAVIS") != null
            && System.getenv("TEST_PAM_USER") != null
            && !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"mariadb-es".equals(System.getenv("srv"))
            && !"mariadb-es-test".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(isMariaDBServer());
    String pamUser = System.getenv("TEST_PAM_USER");
    sharedConn.createStatement("INSTALL PLUGIN pam SONAME 'auth_pam'").execute().blockLast();
    sharedConn.createStatement("DROP USER IF EXISTS '" + pamUser + "'@'%'").execute().blockLast();
    sharedConn
        .createStatement("CREATE USER '" + pamUser + "'@'%' IDENTIFIED VIA pam USING 'mariadb'")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("GRANT SELECT ON *.* TO '" + pamUser + "'@'%' IDENTIFIED VIA pam")
        .execute()
        .blockLast();
    sharedConn.createStatement("FLUSH PRIVILEGES").execute().blockLast();

    int testPort = TestConfiguration.port;
    if (System.getenv("TEST_PAM_PORT") != null) {
      testPort = Integer.parseInt(System.getenv("TEST_PAM_PORT"));
    }

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username(System.getenv("TEST_PAM_USER"))
            .password(System.getenv("TEST_PAM_PWD"))
            .port(testPort)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }
}

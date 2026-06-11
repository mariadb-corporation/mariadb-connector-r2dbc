// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.authentication;

import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.SslMode;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;

public class PamPluginTest extends BaseConnectionTest {

  private void assumePamAvailable() {
    // https://mariadb.com/kb/en/authentication-plugin-pam/
    // only test on travis, because only work on Unix-like operating systems.
    // /etc/pam.d/mariadb pam configuration is created beforehand
    Assumptions.assumeTrue(
        System.getenv("TRAVIS") != null
            && System.getenv("TEST_PAM_USER") != null
            && !isMaxscale()
            && !isEnterprise());
    Assumptions.assumeTrue(isMariaDBServer());
  }

  private void createPamUser() {
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
  }

  private MariadbConnectionConfiguration.Builder pamConfBuilder()
      throws CloneNotSupportedException {
    int testPort = TestConfiguration.port;
    if (System.getenv("TEST_PAM_PORT") != null) {
      testPort = Integer.parseInt(System.getenv("TEST_PAM_PORT"));
    }
    return TestConfiguration.defaultBuilder
        .clone()
        .username(System.getenv("TEST_PAM_USER"))
        .password(System.getenv("TEST_PAM_PWD"))
        .port(testPort);
  }

  @Test
  public void pamAuthPlugin() throws Throwable {
    assumePamAvailable();
    createPamUser();

    // dialog (PAM) is a clear-text password plugin, so it requires a secure connection (R2DBC-118).
    MariadbConnectionConfiguration conf = pamConfBuilder().sslMode(SslMode.TRUST).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }

  @Test
  public void pamAuthPluginRefusedWithoutSsl() throws CloneNotSupportedException {
    // R2DBC-118 (report by fg0x0): dialog (PAM) transmits the password in clear text, so the
    // driver must refuse it over an insecure connection instead of leaking the credentials.
    assumePamAvailable();
    createPamUser();

    MariadbConnectionConfiguration conf = pamConfBuilder().sslMode(SslMode.DISABLE).build();
    R2dbcPermissionDeniedException ex =
        Assertions.assertThrows(
            R2dbcPermissionDeniedException.class,
            () -> new MariadbConnectionFactory(conf).create().block());
    Assertions.assertTrue(
        ex.getMessage().contains("clear-text"), "unexpected message: " + ex.getMessage());
  }
}

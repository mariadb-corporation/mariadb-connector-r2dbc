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
        System.getenv("TRAVIS") != null && System.getenv("MAXSCALE_VERSION") == null);
    Assumptions.assumeTrue(isMariaDBServer());

    sharedConn.createStatement("INSTALL PLUGIN pam SONAME 'auth_pam'").execute().blockLast();
    sharedConn.createStatement("DROP USER IF EXISTS 'testPam'@'%'").execute().blockLast();
    sharedConn
        .createStatement("CREATE USER 'testPam'@'%' IDENTIFIED VIA pam USING 'mariadb'")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("GRANT SELECT ON *.* TO 'testPam'@'%' IDENTIFIED VIA pam")
        .execute()
        .blockLast();
    sharedConn.createStatement("FLUSH PRIVILEGES").execute().blockLast();

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().username("testPam").password("myPwd").build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }
}

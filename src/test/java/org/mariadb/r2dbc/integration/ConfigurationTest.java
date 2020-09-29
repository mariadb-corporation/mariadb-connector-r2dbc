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

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.*;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ConfigurationTest extends BaseTest {

  @Test
  void usingOption() {
    ConnectionFactory factory =
        ConnectionFactories.get(
            String.format(
                "r2dbc:mariadb://%s:%s@%s:%s/%s",
                TestConfiguration.username,
                TestConfiguration.password,
                TestConfiguration.host,
                TestConfiguration.port,
                TestConfiguration.database));
    Connection connection = Mono.from(factory.create()).block();
    Flux.from(connection.createStatement("SELECT * FROM myTable").execute())
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)));
    Mono.from(connection.close()).block();
  }

  @Test
  void ensureUserInfoUrlEncoding() {
    MariadbConnectionFactory factory =
        (MariadbConnectionFactory)
            ConnectionFactories.get(
                "r2dbc:mariadb://root%40%C3%A5:p%40ssword@localhost:3305/%D1" + "%88db");
    Assertions.assertTrue(factory.toString().contains("username='root@å'"));
    Assertions.assertTrue(factory.toString().contains("database='шdb'"));
  }

  @Test
  void checkOptions() {
    MariadbConnectionFactory factory =
        (MariadbConnectionFactory)
            ConnectionFactories.get(
                "r2dbc:mariadb://root:pwd@localhost:3306/db?socket=ff&allowMultiQueries=true&tlsProtocol=TLSv1"
                    + ".2&serverSslCert=myCert&clientSslCert=myClientCert&allowPipelining=true&useServerPrepStmts"
                    + "=true&prepareCacheSize=2560&connectTimeout=PT10S&socketTimeout=PT1H&tcpKeepAlive=true"
                    + "&tcpAbortiveClose=true&sslMode=ENABLE_TRUST"
                    + "&connectionAttributes"
                    + "=test=2,"
                    + "h=4&pamOtherPwd=p%40ssword,pwd");
    Assertions.assertTrue(factory.toString().contains("socket='ff'"));
    Assertions.assertTrue(factory.toString().contains("allowMultiQueries=true"));
    Assertions.assertTrue(factory.toString().contains("tlsProtocol=[TLSv1.2]"));
    Assertions.assertTrue(factory.toString().contains("serverSslCert='myCert'"));
    Assertions.assertTrue(factory.toString().contains("clientSslCert='myClientCert'"));
    Assertions.assertTrue(factory.toString().contains("allowPipelining=true"));
    Assertions.assertTrue(factory.toString().contains("useServerPrepStmts=true"));
    Assertions.assertTrue(factory.toString().contains("prepareCacheSize=2560"));
    Assertions.assertTrue(factory.toString().contains("sslMode=ENABLE_TRUST"));
    Assertions.assertTrue(factory.toString().contains("connectionAttributes={test=2, h=4}"));
    Assertions.assertTrue(factory.toString().contains("pamOtherPwd=******,***"));
    Assertions.assertTrue(factory.toString().contains("connectTimeout=PT10S"));
    Assertions.assertTrue(factory.toString().contains("socketTimeout=PT1H"));
    Assertions.assertTrue(factory.toString().contains("tcpKeepAlive=true"));
    Assertions.assertTrue(factory.toString().contains("tcpAbortiveClose=true"));
  }

  @Test
  void checkNotConcerned() {
    try {
      ConnectionFactories.get("r2dbc:other://root:pwd@localhost:3306/db");
      Assertions.fail();
    } catch (IllegalStateException e) {
      Assertions.assertTrue(e.getMessage().contains("Available drivers:"));
    }
  }

  @Test
  void factory() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "mariadb")
            .option(ConnectionFactoryOptions.HOST, "someHost")
            .option(ConnectionFactoryOptions.PORT, 43306)
            .option(ConnectionFactoryOptions.USER, "myUser")
            .option(ConnectionFactoryOptions.DATABASE, "myDb")
            .option(MariadbConnectionFactoryProvider.ALLOW_MULTI_QUERIES, true)
            .option(MariadbConnectionFactoryProvider.SOCKET_TIMEOUT, Duration.ofSeconds(3600))
            .option(MariadbConnectionFactoryProvider.TCP_KEEP_ALIVE, true)
            .option(MariadbConnectionFactoryProvider.TCP_ABORTIVE_CLOSE, true)
            .option(Option.valueOf("locale"), "en_US")
            .build();
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(options).build();
    MariadbConnectionFactory factory = MariadbConnectionFactory.from(conf);

    Assertions.assertTrue(factory.toString().contains("database='myDb'"));
    Assertions.assertTrue(factory.toString().contains("host='someHost'"));
    Assertions.assertTrue(factory.toString().contains("allowMultiQueries=true"));
    Assertions.assertTrue(factory.toString().contains("allowPipelining=true"));
    Assertions.assertTrue(factory.toString().contains("username='myUser'"));
    Assertions.assertTrue(factory.toString().contains("port=43306"));
    Assertions.assertTrue(factory.toString().contains("connectTimeout=PT10S"));
    Assertions.assertTrue(factory.toString().contains("socketTimeout=PT1H"));
    Assertions.assertTrue(factory.toString().contains("tcpKeepAlive=true"));
    Assertions.assertTrue(factory.toString().contains("tcpAbortiveClose=true"));
  }

  @Test
  void provider() {
    Assertions.assertEquals("mariadb", new MariadbConnectionFactoryProvider().getDriver());
  }

  @Test
  void confError() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "mariadb")
            .option(ConnectionFactoryOptions.PORT, 43306)
            .option(ConnectionFactoryOptions.USER, "myUser")
            .option(ConnectionFactoryOptions.DATABASE, "myDb")
            .option(MariadbConnectionFactoryProvider.ALLOW_MULTI_QUERIES, true)
            .option(Option.valueOf("locale"), "en_US")
            .build();
    assertThrows(
        IllegalStateException.class,
        () -> MariadbConnectionConfiguration.fromOptions(options).build(),
        "No value found for host");
  }

  @Test
  void checkOptionsPerOption() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "mariadb")
            .option(ConnectionFactoryOptions.HOST, "someHost")
            .option(ConnectionFactoryOptions.PORT, 43306)
            .option(ConnectionFactoryOptions.USER, "myUser")
            .option(MariadbConnectionFactoryProvider.ALLOW_MULTI_QUERIES, true)
            .option(Option.valueOf("locale"), "en_US")
            .build();
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(options).build();
    Assertions.assertEquals("someHost", conf.getHost());
    Assertions.assertEquals(43306, conf.getPort());
    Assertions.assertEquals(true, conf.allowMultiQueries());

    final ConnectionFactoryOptions optionsWithoutUser =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "mariadb")
            .option(ConnectionFactoryOptions.HOST, "someHost")
            .option(ConnectionFactoryOptions.PORT, 43306)
            .option(MariadbConnectionFactoryProvider.ALLOW_MULTI_QUERIES, true)
            .option(Option.valueOf("locale"), "en_US")
            .build();
    assertThrows(
        IllegalStateException.class,
        () -> MariadbConnectionConfiguration.fromOptions(optionsWithoutUser).build(),
        "No value found for user");
  }

  @Test
  void confMinOption() {
    assertThrows(
        IllegalArgumentException.class,
        () -> MariadbConnectionConfiguration.builder().build(),
        "host or socket must not be null");
    assertThrows(
        IllegalArgumentException.class,
        () -> MariadbConnectionConfiguration.builder().host("jj").socket("dd").build(),
        "Connection must be configured for either host/port or socket usage but not both");
    assertThrows(
        IllegalArgumentException.class,
        () -> MariadbConnectionConfiguration.builder().host("jj").build(),
        "username must not be null");
  }
}

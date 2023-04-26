// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.*;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ConfigurationTest extends BaseConnectionTest {

  @Test
  void usingOption() {
    String encodedUser;
    String encodedPwd;
    try {
      encodedUser =
          URLEncoder.encode(TestConfiguration.username, StandardCharsets.UTF_8.toString());
      encodedPwd = URLEncoder.encode(TestConfiguration.password, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException e) {
      encodedUser = TestConfiguration.username;
      encodedPwd = TestConfiguration.password;
    }

    ConnectionFactory factory =
        ConnectionFactories.get(
            String.format(
                "r2dbc:mariadb://%s:%s@%s:%s/%s%s",
                encodedUser,
                encodedPwd,
                TestConfiguration.host,
                TestConfiguration.port,
                TestConfiguration.database,
                TestConfiguration.other == null
                    ? ""
                    : "?" + TestConfiguration.other.replace("\n", "\\n")));
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
    Assertions.assertTrue(factory.toString().contains("isolationLevel=null"));
  }

  @Test
  void isolationLevel() {
    MariadbConnectionFactory factory =
        (MariadbConnectionFactory)
            ConnectionFactories.get(
                "r2dbc:mariadb://root:password@localhost:3305/db?isolationLevel=REPEATABLE-READ");
    Assertions.assertTrue(factory.toString().contains("username='root'"));
    Assertions.assertTrue(factory.toString().contains("database='db'"));
    Assertions.assertTrue(
        factory.toString().contains("isolationLevel=IsolationLevel{sql='REPEATABLE READ'}"));
  }

  @Test
  void haMode() {
    MariadbConnectionFactory factory =
        (MariadbConnectionFactory)
            ConnectionFactories.get(
                "r2dbc:mariadb:failover://root:password@localhost:3305/db?isolationLevel=REPEATABLE-READ");
    Assertions.assertTrue(factory.toString().contains("username='root'"));
    Assertions.assertTrue(factory.toString().contains("database='db'"));
    Assertions.assertTrue(
        factory.toString().contains("isolationLevel=IsolationLevel{sql='REPEATABLE READ'}"));
  }

  @Test
  void checkOptions() throws Exception {

    String serverSslCert = System.getenv("TEST_DB_SERVER_CERT");
    String clientSslCert = System.getenv("TEST_DB_CLIENT_CERT");
    String clientSslKey = System.getenv("TEST_DB_CLIENT_KEY");

    // try default if not present
    if (serverSslCert == null) {
      File sslDir = new File(System.getProperty("user.dir") + "/../../ssl");
      if (sslDir.exists() && sslDir.isDirectory()) {

        serverSslCert = System.getProperty("user.dir") + "/../../ssl/server.crt";
        clientSslCert = System.getProperty("user.dir") + "/../../ssl/client.crt";
        clientSslKey = System.getProperty("user.dir") + "/../../ssl/client.key";
      }
    }
    Assumptions.assumeTrue(clientSslCert != null);
    MariadbConnectionFactory factory =
        (MariadbConnectionFactory)
            ConnectionFactories.get(
                "r2dbc:mariadb://root:pwd@localhost:3306/db?socket=ff&allowMultiQueries=true"
                    + "&tlsProtocol=TLSv1.2"
                    + "&serverSslCert="
                    + serverSslCert
                    + "&clientSslCert="
                    + clientSslCert
                    + "&clientSslKey="
                    + clientSslKey
                    + "&allowPipelining=true&useServerPrepStmts"
                    + "=true&prepareCacheSize=2560&connectTimeout=PT10S&tcpKeepAlive=true"
                    + "&tcpAbortiveClose=true&sslMode=TRUST"
                    + "&connectionAttributes"
                    + "=test=2,"
                    + "h=4&pamOtherPwd=p%40ssword,pwd");
    Assertions.assertTrue(factory.toString().contains("socket='ff'"));
    Assertions.assertTrue(factory.toString().contains("allowMultiQueries=true"));
    Assertions.assertTrue(factory.toString().contains("tlsProtocol=[TLSv1.2]"));
    Assertions.assertTrue(factory.toString().contains("serverSslCert=" + serverSslCert));
    Assertions.assertTrue(factory.toString().contains("clientSslCert=" + clientSslCert));
    Assertions.assertTrue(factory.toString().contains("allowPipelining=true"));
    Assertions.assertTrue(factory.toString().contains("useServerPrepStmts=false"));
    Assertions.assertTrue(factory.toString().contains("prepareCacheSize=2560"));
    Assertions.assertTrue(factory.toString().contains("sslMode=TRUST"));
    Assertions.assertTrue(factory.toString().contains("connectionAttributes={test=2, h=4}"));
    Assertions.assertTrue(factory.toString().contains("pamOtherPwd=*,*"));
    Assertions.assertTrue(factory.toString().contains("connectTimeout=PT10S"));
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
  void checkDecoded() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse("r2dbc:mariadb://ro%3Aot:pw%3Ad@localhost:3306/db");
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(options).build();
    Assertions.assertEquals("ro:ot", conf.getUsername());
    Assertions.assertEquals("pw:d", conf.getPassword().toString());
  }

  @Test
  void factory() {

    final ConnectionFactoryOptions option1s = ConnectionFactoryOptions.builder().build();

    assertThrows(
        NoSuchOptionException.class,
        () -> MariadbConnectionConfiguration.fromOptions(option1s).build(),
        "");

    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "mariadb")
            .option(ConnectionFactoryOptions.HOST, "someHost")
            .option(ConnectionFactoryOptions.PORT, 43306)
            .option(ConnectionFactoryOptions.USER, "myUser")
            .option(ConnectionFactoryOptions.DATABASE, "myDb")
            .option(MariadbConnectionFactoryProvider.ALLOW_MULTI_QUERIES, true)
            .option(MariadbConnectionFactoryProvider.TCP_KEEP_ALIVE, true)
            .option(MariadbConnectionFactoryProvider.TCP_ABORTIVE_CLOSE, true)
            .option(Option.valueOf("locale"), "en_US")
            .build();
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(options).build();
    MariadbConnectionFactory factory = MariadbConnectionFactory.from(conf);

    Assertions.assertTrue(factory.toString().contains("database='myDb'"));
    Assertions.assertTrue(
        factory.toString().contains("hosts={[someHost:43306]}"), factory.toString());
    Assertions.assertTrue(factory.toString().contains("allowMultiQueries=true"));
    Assertions.assertTrue(factory.toString().contains("allowPipelining=true"));
    Assertions.assertTrue(factory.toString().contains("username='myUser'"));
    Assertions.assertTrue(factory.toString().contains("connectTimeout=PT10S"));
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
    Assertions.assertEquals("someHost", conf.getHostAddresses().get(0).getHost());
    Assertions.assertEquals(43306, conf.getPort());
    Assertions.assertTrue(conf.allowMultiQueries());

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
  void autocommitValue() throws Exception {
    MariadbConnectionConfiguration conf = TestConfiguration.defaultBuilder.clone().build();

    Assertions.assertTrue(conf.autocommit());

    MariadbConnection sharedConn = new MariadbConnectionFactory(conf).create().block();
    sharedConn
        .createStatement("SELECT @@autocommit")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(1)
        .verifyComplete();
    Assertions.assertTrue(sharedConn.isAutoCommit());
    sharedConn.close().block();

    conf = TestConfiguration.defaultBuilder.clone().autocommit(false).build();
    Assertions.assertFalse(conf.autocommit());
    sharedConn = new MariadbConnectionFactory(conf).create().block();
    Assertions.assertFalse(sharedConn.isAutoCommit());
    sharedConn.createStatement("SET @@autocommit=0");
    sharedConn.close().block();

    conf = TestConfiguration.defaultBuilder.clone().build();
    Assertions.assertTrue(conf.autocommit());
    sharedConn = new MariadbConnectionFactory(conf).create().block();
    Assertions.assertTrue(sharedConn.isAutoCommit());
    sharedConn.createStatement("SET @@autocommit=1");
    sharedConn.close().block();

    Map<String, String> sessionVariables = new HashMap<>();
    sessionVariables.put("net_read_timeout", "60");
    sessionVariables.put("wait_timeout", "2147483");

    conf =
        TestConfiguration.defaultBuilder
            .clone()
            .autocommit(false)
            .sessionVariables(sessionVariables)
            .build();
    Assertions.assertFalse(conf.autocommit());
    sharedConn = new MariadbConnectionFactory(conf).create().block();
    Assertions.assertFalse(sharedConn.isAutoCommit());
    sharedConn.close().block();
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

  @Test
  void sessionVariablesParsing() {
    String connectionUrl =
        "r2dbc:mariadb://admin:pass@localhost:3306/dbname?sessionVariables=sql_mode='ANSI'";
    ConnectionFactoryOptions factoryOptions = ConnectionFactoryOptions.parse(connectionUrl);
    Assertions.assertTrue(factoryOptions.toString().contains("sessionVariables=sql_mode='ANSI'"));
    ConnectionFactory connectionFactory = ConnectionFactories.get(factoryOptions);
    Assertions.assertTrue(
        connectionFactory.toString().contains("sessionVariables={sql_mode='ANSI'}"));
  }

  @Test
  void confStringValue() {
    String connectionUrl =
        "r2dbc:mariadb://admin:pass@localhost:3306/dbname?allowMultiQueries=blabla&autoCommit=1&tinyInt1isBit=0";
    ConnectionFactoryOptions options = ConnectionFactoryOptions.parse(connectionUrl);
    MariadbConnectionConfiguration.Builder builder =
        MariadbConnectionConfiguration.fromOptions(options);
    builder.sslMode(null);
    Assertions.assertTrue(builder.toString().contains("sslMode=DISABLE"));
    builder.sslMode(SslMode.TRUST);
    Assertions.assertTrue(builder.toString().contains("sslMode=TRUST"));
    builder.pamOtherPwd(new String[] {"fff", "ddd"});
    builder.tlsProtocol((String[]) null);
    Assertions.assertEquals(
        "Builder{rsaPublicKey=null, haMode=null, cachingRsaPublicKey=null, allowPublicKeyRetrieval=false, username=admin, connectTimeout=null, tcpKeepAlive=null, tcpAbortiveClose=null, transactionReplay=null, database=dbname, host=localhost, sessionVariables=null, connectionAttributes=null, password=*, restrictedAuth=null, port=3306, hosts={}, socket=null, allowMultiQueries=false, allowPipelining=true, useServerPrepStmts=false, prepareCacheSize=null, isolationLevel=null, tlsProtocol=null, serverSslCert=null, clientSslCert=null, clientSslKey=null, clientSslPassword=null, sslMode=TRUST, sslTunnelDisableHostVerification=false, pamOtherPwd=*,*, tinyInt1isBit=false, autoCommit=true}",
        builder.toString());
    builder.tlsProtocol((String) null);
    Assertions.assertEquals(
        "Builder{rsaPublicKey=null, haMode=null, cachingRsaPublicKey=null, allowPublicKeyRetrieval=false, username=admin, connectTimeout=null, tcpKeepAlive=null, tcpAbortiveClose=null, transactionReplay=null, database=dbname, host=localhost, sessionVariables=null, connectionAttributes=null, password=*, restrictedAuth=null, port=3306, hosts={}, socket=null, allowMultiQueries=false, allowPipelining=true, useServerPrepStmts=false, prepareCacheSize=null, isolationLevel=null, tlsProtocol=null, serverSslCert=null, clientSslCert=null, clientSslKey=null, clientSslPassword=null, sslMode=TRUST, sslTunnelDisableHostVerification=false, pamOtherPwd=*,*, tinyInt1isBit=false, autoCommit=true}",
        builder.toString());
    MariadbConnectionConfiguration conf = builder.build();
    Assertions.assertEquals(
        "SslConfig{sslMode=TRUST, serverSslCert=null, clientSslCert=null, tlsProtocol=null, clientSslKey=null}",
        conf.getSslConfig().toString());
  }

  @Test
  public void emptySessionVariable() throws Exception {
    MariadbConnectionConfiguration emptySessionConf =
        TestConfiguration.defaultBuilder.clone().sessionVariables(new HashMap<>()).build();
    MariadbConnection con = new MariadbConnectionFactory(emptySessionConf).create().block();
    con.close().block();
  }
}

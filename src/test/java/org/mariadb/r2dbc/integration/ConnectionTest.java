// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.spi.*;
import java.math.BigInteger;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.test.StepVerifier;

public class ConnectionTest extends BaseConnectionTest {
  IsolationLevel[] levels =
      new IsolationLevel[] {
        IsolationLevel.READ_UNCOMMITTED,
        IsolationLevel.READ_COMMITTED,
        IsolationLevel.SERIALIZABLE,
        IsolationLevel.REPEATABLE_READ
      };

  @BeforeAll
  public static void before2() {
    dropAll();
    sharedConn.createStatement("CREATE DATABASE test_r2dbc").execute().blockLast();
  }

  @AfterAll
  public static void dropAll() {
    sharedConn.createStatement("DROP DATABASE IF EXISTS test_r2dbc").execute().blockLast();
  }

  @Test
  void localValidation() {
    sharedConn
        .validate(ValidationDepth.LOCAL)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
        .verifyComplete();
  }

  @Test
  void localValidationClosedConnection() {
    MariadbConnection connection = factory.create().block();
    connection.close().block();
    connection
        .validate(ValidationDepth.LOCAL)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
  }

  @Test
  void connectionError() throws Exception {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    // disableLog();
    MariadbConnection connection = createProxyCon();
    proxy.forceClose();
    connection
        .setAutoCommit(false)
        .as(StepVerifier::create)
        .verifyErrorSatisfies(
            t -> {
              assertTrue(t instanceof R2dbcNonTransientResourceException);
              assertTrue(
                  t.getMessage().contains("The connection is closed. Unable to send anything")
                      || t.getMessage()
                          .contains("Cannot execute command since connection is already closed")
                      || t.getMessage().contains("Connection error")
                      || t.getMessage().contains("Connection closed")
                      || t.getMessage().contains("Connection unexpectedly closed")
                      || t.getMessage().contains("Connection unexpected error"));
            });
    Thread.sleep(100);
  }

  @Test
  void validate() {
    MariadbConnection connection = factory.create().block();
    assertTrue(connection.validate(ValidationDepth.LOCAL).block());
    assertTrue(connection.validate(ValidationDepth.REMOTE).block());
    connection.close().block();
    assertFalse(connection.validate(ValidationDepth.LOCAL).block());
    assertFalse(connection.validate(ValidationDepth.REMOTE).block());
  }

  @Test
  void connectionWithoutErrorOnClose() throws Exception {
    Assumptions.assumeTrue(System.getenv("local") == null || "1".equals(System.getenv("local")));

    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnection connection = createProxyCon();
    proxy.stop();
    connection.close().block();
  }

  @Test
  void connectionCollation() throws Exception {
    Assumptions.assumeTrue(
        isMariaDBServer()
            && !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    String defaultUtf8Collation =
        sharedConn
            .createStatement(
                "SELECT COLLATION_NAME FROM information_schema.COLLATIONS c WHERE"
                    + " c.CHARACTER_SET_NAME = 'utf8mb4' AND IS_DEFAULT = 'Yes'")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
            .blockLast();
    System.out.println("default collation:" + defaultUtf8Collation);
    try {
      defaultUtf8Collation =
          sharedConn
              .createStatement(
                  "SELECT COLLATION_NAME FROM"
                      + " information_schema.COLLATION_CHARACTER_SET_APPLICABILITY c WHERE"
                      + " c.CHARACTER_SET_NAME = 'utf8mb4' AND IS_DEFAULT = 'Yes'")
              .execute()
              .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
              .blockLast();
      System.out.println("default collation applicability:" + defaultUtf8Collation);
    } catch (Exception e) {
      // eat - for mariadb 11.3+ only
    }

    MariadbConnection connection = factory.create().block();
    try {
      String newDefaultCollation =
          connection
              .createStatement("SELECT @@collation_connection")
              .execute()
              .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
              .blockLast();
      Assertions.assertTrue(
          defaultUtf8Collation.equals(newDefaultCollation)
              || ("utf8mb4_" + defaultUtf8Collation).equals(newDefaultCollation));
      connection.close().block();

      MariadbConnectionConfiguration confPipeline =
          TestConfiguration.defaultBuilder.clone().collation("utf8mb4_nopad_bin").build();
      connection = new MariadbConnectionFactory(confPipeline).create().block();
      newDefaultCollation =
          connection
              .createStatement("SELECT @@collation_connection")
              .execute()
              .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
              .blockLast();
      Assertions.assertEquals("utf8mb4_nopad_bin", newDefaultCollation);

    } finally {
      connection.close().block();
    }
  }

  //    @Test
  //    void perf() {
  //      for (int ii = 0; ii < 1000000; ii++) {
  //        io.r2dbc.spi.Statement statement = sharedConn.createStatement(sql);
  //        for (int i = 0; i < 1000; i++) statement.bind(i, i);
  //
  //        Flux.from(statement.execute()).flatMap(it -> it.getRowsUpdated()).blockLast();
  //      }
  //    }

  //  @Test
  //  void perf() {
  //    for (int ii = 0; ii < 1000000; ii++) {
  //      io.r2dbc.spi.Statement statement = sharedConn.createStatement("DO 1");
  //      Flux.from(statement.execute()).flatMap(it -> it.getRowsUpdated()).blockLast();
  //    }
  //  }

  @Test
  @Timeout(5)
  void connectionDuringError() throws Exception {
    Assumptions.assumeTrue(System.getenv("local") == null || "1".equals(System.getenv("local")));

    Assumptions.assumeTrue(!isMaxscale() && !isEnterprise());
    MariadbConnection connection = createProxyCon();
    new Timer()
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                proxy.forceClose();
              }
            },
            200);

    connection
        .createStatement(
            "select * from information_schema.columns as c1, "
                + "information_schema.tables, information_schema.tables as t2")
        .execute()
        .flatMap(r -> r.map((rows, meta) -> ""))
        .onErrorComplete()
        .blockLast();
    connection
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
    Thread.sleep(100);
  }

  @Test
  void remoteValidation() {
    sharedConn
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
        .verifyComplete();
  }

  @Test
  void remoteValidationClosedConnection() {
    MariadbConnection connection = factory.create().block();
    assert connection != null;
    connection.close().block();
    connection
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
  }

  @Test
  void multipleConnection() {
    for (int i = 0; i < 50; i++) {
      MariadbConnection connection = factory.create().block();
      connection
          .validate(ValidationDepth.REMOTE)
          .as(StepVerifier::create)
          .expectNext(Boolean.TRUE)
          .verifyComplete();
      connection.close().block();
    }
  }

  @Test
  void connectTimeout() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().connectTimeout(Duration.ofSeconds(1)).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    connection.close().block();
  }

  @Test
  void timeoutMultiHost() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .host(
                "128.2.2.2,"
                    + TestConfiguration.defaultBuilder
                        .clone()
                        .build()
                        .getHostAddresses()
                        .get(0)
                        .getHost())
            .connectTimeout(Duration.ofMillis(500))
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    connection.close().block();
  }

  @Test
  public void localSocket() {
    Assumptions.assumeTrue(
        System.getenv("local") != null
            && "1".equals(System.getenv("local"))
            && !System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
    String socket =
        sharedConn
            .createStatement("select @@socket")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
            .blockLast();
    sharedConn.createStatement("DROP USER IF EXISTS testSocket@'localhost'").execute().blockLast();
    sharedConn
        .createStatement("CREATE USER testSocket@'localhost' IDENTIFIED BY 'MySup5%rPassw@ord'")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "GRANT SELECT on *.* to testSocket@'localhost' IDENTIFIED BY 'MySup5%rPassw@ord'")
        .execute()
        .blockLast();
    sharedConn.createStatement("FLUSH PRIVILEGES").execute().blockLast();

    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder()
            .username("testSocket")
            .password("MySup5%rPassw@ord")
            .database(TestConfiguration.database)
            .socket(socket)
            .build();

    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    assert connection != null;
    connection.close().block();

    sharedConn.createStatement("DROP USER testSocket@'localhost'").execute().blockLast();
  }

  @Test
  void socketTcpKeepAlive() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().tcpKeepAlive(Boolean.TRUE).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    assert connection != null;
    connection.close().block();
  }

  @Test
  void socketTcpAbortiveClose() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().tcpAbortiveClose(Boolean.TRUE).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    assert connection != null;
    connection.close().block();
  }

  @Test
  void basicConnectionWithoutPipeline() throws Exception {
    MariadbConnectionConfiguration noPipeline =
        TestConfiguration.defaultBuilder
            .clone()
            .allowPipelining(false)
            .useServerPrepStmts(true)
            .prepareCacheSize(1)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(noPipeline).create().block();
    connection
        .createStatement("SELECT 5")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(5)
        .verifyComplete();
    connection
        .createStatement("SELECT 6")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(6)
        .verifyComplete();
    connection
        .createStatement("SELECT ?")
        .bind(0, 7)
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(7)
        .verifyComplete();
    connection
        .createStatement("SELECT 1, ?")
        .bind(0, 8)
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(1, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(8)
        .verifyComplete();
    connection.close().block();
    connection
        .createStatement("SELECT 7")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable
                            .getMessage()
                            .equals("The connection is closed. Unable to send anything")
                        || throwable
                            .getMessage()
                            .contains("Cannot execute command since connection is already closed")))
        .verify();
  }

  @Test
  void basicConnectionWithSessionVariable() throws Exception {
    Map<String, Object> sessionVariable = new HashMap<>();
    sessionVariable.put("collation_connection", "utf8_slovenian_ci");
    sessionVariable.put("wait_timeout", 3600);
    MariadbConnectionConfiguration cnf =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionVariable).build();
    MariadbConnection connection = new MariadbConnectionFactory(cnf).create().block();
    connection
        .createStatement("SELECT 5")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(5)
        .verifyComplete();
    connection.close().block();

    sessionVariable.put("test", null);
    MariadbConnectionConfiguration cnf2 =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionVariable).build();
    try {
      new MariadbConnectionFactory(cnf2).create().block();
      Assertions.fail("must have throw exception");
    } catch (Throwable t) {
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getClass());
      Assertions.assertNotNull(t.getCause());
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getCause().getClass());
      Assertions.assertTrue(
          t.getCause().getMessage().contains("Session variable 'test' has no value"));
    }
  }

  @Test
  void multipleClose() {
    MariadbConnection connection = factory.create().block();
    connection.close().subscribe();
    connection.close().block();
  }

  @Test
  void withTimezoneMinus8() throws Exception {
    MariadbConnection connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().timezone("GMT-8").build())
            .create()
            .block();
    connection
        .createStatement("SELECT @@time_zone")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("-08:00")
        .verifyComplete();

    connection.close().block();
  }

  @Test
  void withTimezoneUtc() throws Exception {
    MariadbConnection connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().timezone("UTC").build())
            .create()
            .block();
    connection
        .createStatement("SELECT @@time_zone, @@system_time_zone")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      String srvTz = row.get(0, String.class);
                      if ("SYSTEM".equals(srvTz)) {
                        return row.get(1, String.class);
                      }
                      return srvTz;
                    }))
        .as(StepVerifier::create)
        .expectNextMatches(srvTz -> "+00:00".equals(srvTz) || "UTC".equals(srvTz))
        .verifyComplete();

    connection.close().block();
  }

  @Test
  void multipleBegin() throws Exception {
    MariadbConnection connection = factory.create().block();
    assert connection != null;
    multipleBegin(connection);
    connection.close().block();

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().allowPipelining(false).build())
            .create()
            .block();
    assert connection != null;
    multipleBegin(connection);
    connection.close().block();
  }

  void multipleBegin(MariadbConnection con) throws Exception {
    con.beginTransaction().subscribe();
    con.beginTransaction().block();
    con.beginTransaction().block();
    con.rollbackTransaction().block();
  }

  @Test
  void multipleBeginWithIsolation() throws Exception {
    MariadbTransactionDefinition[] transactionDefinitions = {
      MariadbTransactionDefinition.READ_ONLY,
      MariadbTransactionDefinition.READ_WRITE,
      MariadbTransactionDefinition.EMPTY,
      MariadbTransactionDefinition.WITH_CONSISTENT_SNAPSHOT_READ_ONLY,
      MariadbTransactionDefinition.WITH_CONSISTENT_SNAPSHOT_READ_WRITE
    };

    for (MariadbTransactionDefinition transactionDefinition : transactionDefinitions) {
      MariadbConnection connection = factory.create().block();
      assert connection != null;
      multipleBeginWithIsolation(connection, transactionDefinition);
      connection.close().block();

      connection =
          new MariadbConnectionFactory(
                  TestConfiguration.defaultBuilder.clone().allowPipelining(false).build())
              .create()
              .block();
      assert connection != null;
      multipleBeginWithIsolation(connection, transactionDefinition);
      connection.close().block();
    }
  }

  void multipleBeginWithIsolation(
      MariadbConnection con, MariadbTransactionDefinition transactionDefinition) throws Exception {
    con.beginTransaction(transactionDefinition).subscribe();
    con.beginTransaction(transactionDefinition).block();
    con.beginTransaction(transactionDefinition).block();
    con.rollbackTransaction().block();
  }

  @Test
  void beginTransactionWithIsolation() throws Exception {
    Assumptions.assumeTrue(!isEnterprise() && !minVersion(10, 2, 0));
    TransactionDefinition transactionDefinition =
        MariadbTransactionDefinition.READ_ONLY.isolationLevel(IsolationLevel.READ_COMMITTED);
    TransactionDefinition transactionDefinition2 =
        MariadbTransactionDefinition.READ_ONLY.isolationLevel(IsolationLevel.REPEATABLE_READ);
    assertFalse(sharedConn.isInTransaction());

    sharedConn.beginTransaction(transactionDefinition).block();
    assertEquals(IsolationLevel.READ_COMMITTED, sharedConn.getTransactionIsolationLevel());
    assertTrue(sharedConn.isInTransaction());
    assertTrue(sharedConn.isInReadOnlyTransaction());
    sharedConn.beginTransaction(transactionDefinition).block();
    sharedConn
        .beginTransaction(transactionDefinition2)
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcPermissionDeniedException
                    && throwable
                        .getMessage()
                        .equals(
                            "Transaction characteristics can't be changed while a transaction is"
                                + " in progress"))
        .verify();
    sharedConn.rollbackTransaction().block();
  }

  @Test
  void multipleAutocommit() throws Exception {
    MariadbConnection connection = factory.create().block();
    assert connection != null;
    multipleAutocommit(connection);
    connection.close().block();

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().allowPipelining(false).build())
            .create()
            .block();
    assert connection != null;
    multipleAutocommit(connection);
    connection.close().block();
  }

  void multipleAutocommit(MariadbConnection con) throws Exception {
    con.setAutoCommit(true).subscribe();
    con.setAutoCommit(true).block();
    con.setAutoCommit(false).block();
    con.setAutoCommit(true).block();
  }

  @Test
  void queryAfterClose() throws Exception {
    MariadbConnection connection = factory.create().block();
    MariadbStatement stmt = connection.createStatement("SELECT 1");
    connection.close().block();
    stmt.execute()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable
                            .getMessage()
                            .contains("The connection is closed. Unable to send anything")
                        || throwable
                            .getMessage()
                            .contains("Cannot execute command since connection is already closed")))
        .verify();
  }

  private void consume(Connection connection) {
    if (isXpand()) {
      Statement statement = connection.createStatement("select 1");
      Flux.from(statement.execute())
          .flatMap(it -> it.map((row, rowMetadata) -> row.get(0)))
          .blockLast();

    } else {
      int numberOfUserCol = 41;
      Statement statement = connection.createStatement("select * FROM mysql.user LIMIT 1");
      Flux.from(statement.execute())
          .flatMap(
              it ->
                  it.map(
                      (row, rowMetadata) -> {
                        Object[] objs = new Object[numberOfUserCol];
                        for (int i = 0; i < numberOfUserCol; i++) {
                          objs[i] = row.get(i);
                        }
                        return objs;
                      }))
          .blockLast();
    }
  }

  @Test
  void multiThreading() throws Throwable {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    List<ExecuteQueries> queries = new ArrayList<>(100);
    try {
      AtomicInteger completed = new AtomicInteger(0);
      ThreadPoolExecutor scheduler =
          new ThreadPoolExecutor(10, 20, 50, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

      for (int i = 0; i < 100; i++) {
        ExecuteQueries exec = new ExecuteQueries(completed);
        queries.add(exec);
        scheduler.execute(exec);
      }
      scheduler.shutdown();
      scheduler.awaitTermination(120, TimeUnit.SECONDS);
      Assertions.assertEquals(100, completed.get());
    } finally {
      for (ExecuteQueries exec : queries) exec.closeConnection();
    }
  }

  @Test
  @Timeout(120)
  void multiThreadingSameConnection() throws Throwable {
    MariadbConnection connection = factory.create().block();
    try {
      AtomicInteger completed = new AtomicInteger(0);
      ThreadPoolExecutor scheduler =
          new ThreadPoolExecutor(10, 20, 50, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

      for (int i = 0; i < 10; i++) {
        scheduler.execute(new ExecuteQueriesOnSameConnection(completed, connection));
      }
      scheduler.shutdown();
      scheduler.awaitTermination(100, TimeUnit.SECONDS);
      Assertions.assertEquals(10, completed.get());
    } finally {
      assert connection != null;
      connection.close().block();
    }
  }

  @Test
  void connectionAttributes() throws Exception {

    Map<String, String> connectionAttributes = new HashMap<>();
    connectionAttributes.put("APPLICATION", "MyApp");
    connectionAttributes.put("OTHER", "OTHER information");

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().connectionAttributes(connectionAttributes).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    assert connection != null;
    connection.close().block();
  }

  @Test
  void sessionVariables() throws Exception {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 2, 0));
    BigInteger[] res =
        sharedConn
            .createStatement("SELECT @@wait_timeout, @@net_read_timeout")
            .execute()
            .flatMap(
                r ->
                    r.map(
                        (row, metadata) ->
                            new BigInteger[] {
                              row.get(0, BigInteger.class), row.get(1, BigInteger.class)
                            }))
            .blockLast();

    Map<String, Object> sessionVariables = new HashMap<>();
    sessionVariables.put("max_statement_time", 60.5);
    sessionVariables.put("wait_timeout", 2147483);

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionVariables).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    assert connection != null;
    connection
        .createStatement("SELECT @@wait_timeout, @@max_statement_time")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Assertions.assertEquals(row.get(0, BigInteger.class).intValue(), 2147483);
                      Assertions.assertEquals(row.get(1, Float.class), 60.5f);
                      Assertions.assertNotEquals(
                          row.get(0, BigInteger.class).intValue(), res[0].intValue());
                      Assertions.assertNotEquals(row.get(1, Float.class), res[1].floatValue());
                      return 0;
                    }))
        .blockLast();

    connection.close().block();
  }

  @Test
  void getTransactionIsolationLevel() {
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();
    try {
      IsolationLevel defaultValue = IsolationLevel.REPEATABLE_READ;
      Assertions.assertEquals(defaultValue, connection.getTransactionIsolationLevel());
      connection.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED).block();
      connection.createStatement("BEGIN").execute().blockLast();
      Assertions.assertEquals(
          IsolationLevel.READ_UNCOMMITTED, connection.getTransactionIsolationLevel());
      connection.setTransactionIsolationLevel(defaultValue).block();
    } finally {
      connection.close().block();
    }
  }

  @Test
  void getDatabase() {
    Assumptions.assumeFalse(isXpand());
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();
    assertEquals(TestConfiguration.database, connection.getDatabase());
    connection.setDatabase("test_r2dbc").block();
    assertEquals("test_r2dbc", connection.getDatabase());
    String db =
        connection
            .createStatement("SELECT DATABASE()")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, String.class)))
            .blockFirst();
    assertEquals("test_r2dbc", db);
    connection.close().block();
  }

  @Test
  void rollbackTransaction() {
    sharedConn.createStatement("DROP TABLE IF EXISTS rollbackTable").execute().blockLast();
    sharedConn
        .createStatement("CREATE TABLE rollbackTable (t1 VARCHAR(256))")
        .execute()
        .blockLast();
    sharedConn.setAutoCommit(false).block();
    sharedConn.rollbackTransaction().block(); // must not do anything
    sharedConn.createStatement("INSERT INTO rollbackTable VALUES ('a')").execute().blockLast();
    sharedConn.rollbackTransaction().block();
    sharedConn
        .createStatement("SELECT * FROM rollbackTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .verifyComplete();
    sharedConn.createStatement("DROP TABLE IF EXISTS rollbackTable").execute().blockLast();
    sharedConn.setAutoCommit(true).block();
  }

  @Test
  void commitTransaction() throws Exception {
    MariadbConnection connection = factory.create().block();
    commitTransaction(connection);
    MariadbStatement stmt = connection.createStatement("DO 1");
    connection.close().block();
    assertThrows(R2dbcNonTransientResourceException.class, () -> stmt.execute().blockLast(), "");

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().allowPipelining(false).build())
            .create()
            .block();
    commitTransaction(connection);
    MariadbStatement stmt2 = connection.createStatement("DO 1");
    connection.close().block();
    assertThrows(R2dbcNonTransientResourceException.class, () -> stmt2.execute().blockLast(), "");
  }

  void commitTransaction(MariadbConnection con) {
    con.createStatement("DROP TABLE IF EXISTS commitTransaction").execute().blockLast();
    con.createStatement("CREATE TABLE commitTransaction (t1 VARCHAR(256))").execute().blockLast();
    con.setAutoCommit(false).subscribe();
    con.setAutoCommit(false).block();
    con.commitTransaction().block(); // must not do anything
    con.createStatement("INSERT INTO commitTransaction VALUES ('a')").execute().blockLast();
    con.commitTransaction().subscribe();
    con.commitTransaction().block();
    con.createStatement("SELECT * FROM commitTransaction")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("a"))
        .verifyComplete();
    con.createStatement("DROP TABLE IF EXISTS commitTransaction").execute().blockLast();
    con.setAutoCommit(true).block();
  }

  @Test
  void useTransaction() throws Exception {
    MariadbConnection connection = factory.create().block();
    useTransaction(connection);
    MariadbStatement stmt = connection.createStatement("DO 1");
    connection.close().block();
    assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> stmt.execute().blockLast(),
        "The connection is closed. Unable to send anything");

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().allowPipelining(false).build())
            .create()
            .block();
    useTransaction(connection);
    MariadbStatement stmt2 = connection.createStatement("DO 1");
    connection.close().block();
    assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> stmt2.execute().blockLast(),
        "The connection is closed. Unable to send anything");
  }

  void useTransaction(MariadbConnection conn) {
    conn.createStatement("DROP TABLE IF EXISTS useTransaction").execute().blockLast();
    conn.createStatement("CREATE TABLE useTransaction (t1 VARCHAR(256))").execute().blockLast();
    conn.beginTransaction().subscribe();
    conn.beginTransaction().block();
    conn.createStatement("INSERT INTO useTransaction VALUES ('a')").execute().blockLast();
    conn.commitTransaction().block();
    conn.createStatement("SELECT * FROM useTransaction")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("a"))
        .verifyComplete();
    conn.createStatement("DROP TABLE IF EXISTS useTransaction").execute().blockLast();
  }

  @Test
  void wrongSavePoint() {
    sharedConn.createStatement("START TRANSACTION").execute().blockLast();
    try {
      sharedConn.rollbackTransactionToSavepoint("wrong").block();
      Assertions.fail("Must have thrown error");
    } catch (Exception e) {
      Assertions.assertTrue(
          e.getMessage().contains("Savepoint does not exist")
              || e.getMessage().contains("SAVEPOINT wrong does not exist"));
    }
    sharedConn.rollbackTransaction().block();
  }

  @Test
  void useSavePoint() {
    sharedConn.createStatement("DROP TABLE IF EXISTS useSavePoint").execute().blockLast();
    sharedConn.createStatement("CREATE TABLE useSavePoint (t1 VARCHAR(256))").execute().blockLast();
    Assertions.assertTrue(sharedConn.isAutoCommit());
    sharedConn.setAutoCommit(false).block();
    Assertions.assertFalse(sharedConn.isAutoCommit());
    sharedConn.createSavepoint("point1").block();
    sharedConn.releaseSavepoint("point1").block();
    sharedConn.createSavepoint("point2").block();
    sharedConn.createStatement("INSERT INTO useSavePoint VALUES ('a')").execute().blockLast();
    sharedConn.rollbackTransactionToSavepoint("point2").block();
    sharedConn.commitTransaction().block();
    sharedConn
        .createStatement("SELECT * FROM useSavePoint")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        //        .expectNext(Optional.of("a"))
        .verifyComplete();
    sharedConn.createStatement("DROP TABLE IF EXISTS useSavePoint").execute().blockLast();
    sharedConn.setAutoCommit(true).block();
  }

  @Test
  void toStringTest() {
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();
    try {
      Assertions.assertTrue(
          connection
              .toString()
              .contains(
                  "MariadbConnection{client=Client{isClosed=false, "
                      + "context=ConnectionContext{"));
      if (!"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv"))) {
        Assertions.assertTrue(
            connection
                .toString()
                .contains(", isolationLevel=IsolationLevel{sql='REPEATABLE READ'}}"),
            connection.toString());
      }
    } finally {
      connection.close().block();
    }
  }

  @Test
  public void isolationLevel() {
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();

    Assertions.assertThrows(
        Exception.class, () -> connection.setTransactionIsolationLevel(null).block());
    for (IsolationLevel level : levels) {
      System.out.println(level);
      connection.setTransactionIsolationLevel(level).block();
      assertEquals(level, connection.getTransactionIsolationLevel());
    }
    connection.close().block();
    Assertions.assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> connection.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED).block());
  }

  @Test
  public void noDb() throws Throwable {
    MariadbConnection connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().database(null).build())
            .create()
            .block();
    try {
      connection
          .createStatement("SELECT DATABASE()")
          .execute()
          .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
          .as(StepVerifier::create)
          .expectNext(Optional.empty())
          .verifyComplete();
    } finally {
      connection.close().block();
    }
  }

  @Test
  public void initialIsolationLevel() throws CloneNotSupportedException {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    for (IsolationLevel level : levels) {
      sharedConn
          .createStatement("SET GLOBAL TRANSACTION ISOLATION LEVEL " + level.asSql())
          .execute()
          .blockLast();
      MariadbConnection connection =
          new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();
      assertEquals(IsolationLevel.REPEATABLE_READ, connection.getTransactionIsolationLevel());
      connection.close().block();

      connection =
          new MariadbConnectionFactory(
                  TestConfiguration.defaultBuilder.clone().isolationLevel(level).build())
              .create()
              .block();
      assertEquals(level, connection.getTransactionIsolationLevel());
      String sql = "SELECT @@tx_isolation";

      if (!isMariaDBServer()) {
        if ((minVersion(8, 0, 3))
            || (sharedConn.getMetadata().getMajorVersion() < 8 && minVersion(5, 7, 20))) {
          sql = "SELECT @@transaction_isolation";
        }
      }

      String iso =
          connection
              .createStatement(sql)
              .execute()
              .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, String.class)))
              .blockFirst();
      assertEquals(level, IsolationLevel.valueOf(iso.replace("-", " ")));
      connection.close().block();
    }

    sharedConn
        .createStatement(
            "SET GLOBAL TRANSACTION ISOLATION LEVEL " + IsolationLevel.REPEATABLE_READ.asSql())
        .execute()
        .blockLast();
  }

  @Test
  public void errorOnConnection() throws Throwable {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql-ha".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !isXpand());

    BigInteger maxConn =
        sharedConn
            .createStatement("select @@max_connections")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, BigInteger.class)))
            .blockLast();
    Assumptions.assumeTrue(maxConn.intValue() < 600);

    Throwable expected = null;
    Mono<?>[] cons = new Mono<?>[maxConn.intValue()];
    for (int i = 0; i < maxConn.intValue(); i++) {
      cons[i] = new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create();
    }
    MariadbConnection[] connections = new MariadbConnection[maxConn.intValue()];
    for (int i = 0; i < maxConn.intValue(); i++) {
      try {
        connections[i] = (MariadbConnection) cons[i].block();
      } catch (Throwable e) {
        expected = e;
      }
    }

    for (int i = 0; i < maxConn.intValue(); i++) {
      if (connections[i] != null) {
        connections[i].close().block();
      }
    }
    Assertions.assertNotNull(expected);
    Assertions.assertTrue(expected.getMessage().contains("Fail to establish connection to"));
    Assertions.assertTrue(expected.getCause().getMessage().contains("Too many connections"));
    Thread.sleep(1000);
  }

  @Test
  @Timeout(2)
  void killedConnection() {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnection connection = factory.create().block();
    long threadId = connection.getThreadId();
    assertNotNull(connection.getHost());
    assertEquals(TestConfiguration.defaultBuilder.build().getPort(), connection.getPort());

    Runnable runnable =
        () -> {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          sharedConn.createStatement("KILL CONNECTION " + threadId).execute().blockLast();
        };
    Thread thread = new Thread(runnable);
    thread.start();

    try {
      connection
          .createStatement(
              "select * from information_schema.columns as c1, "
                  + "information_schema.tables, information_schema.tables as t2")
          .execute()
          .flatMap(r -> r.map((rows, meta) -> ""))
          .blockLast();
      Assertions.fail("must have throw exception");
    } catch (Throwable t) {
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getClass());
      Assertions.assertTrue(
          t.getMessage().contains("The connection is closed. Unable to send anything")
              || t.getMessage().contains("Connection unexpectedly closed")
              || t.getMessage().contains("Connection unexpected error")
              || t.getMessage().contains("Connection error"),
          "real msg:" + t.getMessage());
      connection
          .validate(ValidationDepth.LOCAL)
          .as(StepVerifier::create)
          .expectNext(Boolean.FALSE)
          .verifyComplete();
    }
    connection
        .validate(ValidationDepth.LOCAL)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
  }

  @Test
  @Disabled
  public void queryTimeout() throws Throwable {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && !isXpand());
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.clone().build())
            .create()
            .block();
    connection.setStatementTimeout(Duration.ofMillis(500)).block();

    try {
      connection
          .createStatement(
              "select * from information_schema.columns as c1, "
                  + "information_schema.tables, information_schema.tables as t2")
          .execute()
          .flatMap(r -> r.map((rows, meta) -> ""))
          .blockLast();
      Assertions.fail();
    } catch (R2dbcTimeoutException e) {
      assertTrue(
          e.getMessage().contains("Query execution was interrupted (max_statement_time exceeded)")
              || e.getMessage()
                  .contains(
                      "Query execution was interrupted, maximum statement execution time"
                          + " exceeded"));
    } finally {
      connection.close().block();
    }
  }

  @Test
  public void setLockWaitTimeout() {
    sharedConn.setLockWaitTimeout(Duration.ofMillis(1)).block();
  }

  @Test
  public void testPools() throws Throwable {
    boolean hasReactorTcp = false;
    boolean hasMariaDbThreads = false;
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    for (Thread thread : threadSet) {
      if (thread.getName().contains("reactor-tcp")) hasReactorTcp = true;
      if (thread.getName().contains("mariadb")) hasMariaDbThreads = true;
    }
    assertTrue(hasReactorTcp);
    assertFalse(hasMariaDbThreads);

    MariadbConnection connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder
                    .clone()
                    .loopResources(LoopResources.create("mariadb"))
                    .build())
            .create()
            .block();

    threadSet = Thread.getAllStackTraces().keySet();
    for (Thread thread : threadSet) {
      if (thread.getName().contains("mariadb")) {
        hasMariaDbThreads = true;
        break;
      }
    }
    assertTrue(hasMariaDbThreads);

    connection.close().block();
  }

  protected class ExecuteQueries implements Runnable {
    private final AtomicInteger i;
    private MariadbConnection connection = null;

    public ExecuteQueries(AtomicInteger i) {
      this.i = i;
    }

    public void run() {
      try {
        connection = factory.create().block();
        int rnd = (int) (Math.random() * 1000);
        Statement statement = connection.createStatement("select " + rnd);
        BigInteger val =
            Flux.from(statement.execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, BigInteger.class)))
                .blockLast();
        if (rnd != val.intValue())
          throw new IllegalStateException("ERROR rnd:" + rnd + " different to val:" + val);
        i.incrementAndGet();
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        closeConnection();
      }
    }

    public synchronized void closeConnection() {
      if (connection != null) {
        connection.close().block();
        connection = null;
      }
    }
  }

  protected class ExecuteQueriesOnSameConnection implements Runnable {
    private final AtomicInteger i;
    private final MariadbConnection connection;

    public ExecuteQueriesOnSameConnection(AtomicInteger i, MariadbConnection connection) {
      this.i = i;
      this.connection = connection;
    }

    public void run() {
      try {
        int rnd = (int) (Math.random() * 1000);
        Statement statement = connection.createStatement("select " + rnd);
        Integer val =
            Flux.from(statement.execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
                .blockLast();
        if (rnd != val.intValue())
          throw new IllegalStateException("ERROR rnd:" + rnd + " different to val:" + val);
        i.incrementAndGet();
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }
}

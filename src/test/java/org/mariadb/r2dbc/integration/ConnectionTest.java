// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import ch.qos.logback.classic.Level;
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
import org.mariadb.r2dbc.api.MariadbResult;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.test.StepVerifier;

public class ConnectionTest extends BaseConnectionTest {
  private Level initialReactorLvl;
  private Level initialLvl;

  @BeforeAll
  public static void before2() {
    dropAll();
    sharedConn.createStatement("CREATE DATABASE test_r2dbc").execute().blockLast();
  }

  @AfterAll
  public static void dropAll() {
    sharedConn.createStatement("DROP DATABASE test_r2dbc").execute().blockLast();
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

  private void disableLog() {
    ch.qos.logback.classic.Logger driver =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("org.mariadb.r2dbc");
    ch.qos.logback.classic.Logger reactor =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("reactor.core");

    initialReactorLvl = reactor.getLevel();
    initialLvl = driver.getLevel();
    driver.setLevel(Level.OFF);
    reactor.setLevel(Level.OFF);
  }

  private void reInitLog() {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("org.mariadb.r2dbc");
    ch.qos.logback.classic.Logger r2dbc =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("io.r2dbc.spi");
    root.setLevel(initialReactorLvl);
    r2dbc.setLevel(initialReactorLvl);
  }

  @Test
  void connectionError() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    // disableLog();
    MariadbConnection connection = createProxyCon();
    try {
      proxy.stop();
      connection.setAutoCommit(false).block();
      Assertions.fail("must have throw exception");
    } catch (Throwable t) {
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getClass());
      Assertions.assertTrue(
          t.getMessage().contains("Connection is close. Cannot send anything")
              || t.getMessage().contains("Connection unexpectedly closed")
              || t.getMessage().contains("Connection unexpected error"),
          "real msg:" + t.getMessage());
    } finally {
      Thread.sleep(100);
      // reInitLog();
    }
  }

  @Test
  void multipleCommandStack() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    // disableLog();
    MariadbConnection connection = createProxyCon();
    Runnable runnable = () -> proxy.stop();
    Thread th = new Thread(runnable);

    try {
      List<Flux<MariadbResult>> results = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        results.add(connection.createStatement("SELECT * from mysql.user").execute());
      }
      for (int i = 0; i < 50; i++) {
        results.get(i).subscribe();
      }
      th.start();

    } catch (Throwable t) {
      Assertions.assertNotNull(t.getCause());
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getCause().getClass());
      Assertions.assertTrue(
          t.getCause().getMessage().contains("Connection is close. Cannot send anything")
              || t.getCause().getMessage().contains("Connection unexpectedly closed")
              || t.getCause().getMessage().contains("Connection unexpected error"),
          "real msg:" + t.getCause().getMessage());
    } finally {
      Thread.sleep(100);
      // reInitLog();
      proxy.forceClose();
    }
  }

  @Test
  void connectionWithoutErrorOnClose() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    //    disableLog();
    MariadbConnection connection = createProxyCon();
    proxy.stop();
    connection.close().block();
    //    reInitLog();
  }

  @Test
  void connectionDuringError() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    //    disableLog();
    MariadbConnection connection = createProxyCon();
    new Timer()
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                proxy.stop();
              }
            },
            200);

    assertTimeout(
        Duration.ofSeconds(5),
        () -> {
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
                t.getMessage().contains("Connection is close. Cannot send anything")
                    || t.getMessage().contains("Connection unexpectedly closed")
                    || t.getMessage().contains("Connection unexpected error"),
                "real msg:" + t.getMessage());
            connection
                .validate(ValidationDepth.LOCAL)
                .as(StepVerifier::create)
                .expectNext(Boolean.FALSE)
                .verifyComplete();
            //            reInitLog();
          }
        });
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
  void socketTimeout() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().socketTimeout(Duration.ofSeconds(1)).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    connection.close().block();
  }

  @Test
  void socketTimeoutMultiHost() throws Exception {
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
  public void localSocket() throws Exception {
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
    connection.close().block();

    sharedConn.createStatement("DROP USER testSocket@'localhost'").execute().blockLast();
  }

  @Test
  void socketTcpKeepAlive() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().tcpKeepAlive(Boolean.TRUE).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    connection.close().block();
  }

  @Test
  void socketTcpAbortiveClose() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().tcpAbortiveClose(Boolean.TRUE).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
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
                    && throwable.getMessage().equals("Connection is close. Cannot send anything"))
        .verify();
  }

  @Test
  void basicConnectionWithSessionVariable() throws Exception {
    Map<String, String> sessionVariable = new HashMap<>();
    sessionVariable.put("collation_connection", "utf8_slovenian_ci");
    sessionVariable.put("wait_timeout", "3600");
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

    sessionVariable.put("test", null);
    MariadbConnectionConfiguration cnf2 =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionVariable).build();
    try {
      new MariadbConnectionFactory(cnf2).create().block();
      Assertions.fail("must have throw exception");
    } catch (Throwable t) {
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getClass());
      Assertions.assertNotNull(t.getCause());
      Assertions.assertEquals(IllegalArgumentException.class, t.getCause().getClass());
      Assertions.assertTrue(
          t.getCause().getMessage().contains("Session variable 'test' has no value"));
    }
  }

  @Test
  void multipleClose() throws Exception {
    MariadbConnection connection = factory.create().block();
    connection.close().subscribe();
    connection.close().block();
  }

  @Test
  void multipleBegin() throws Exception {
    MariadbConnection connection = factory.create().block();
    multipleBegin(connection);
    connection.close().block();

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().allowPipelining(false).build())
            .create()
            .block();
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
      multipleBeginWithIsolation(connection, transactionDefinition);
      connection.close().block();

      connection =
          new MariadbConnectionFactory(
                  TestConfiguration.defaultBuilder.clone().allowPipelining(false).build())
              .create()
              .block();
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
                            "Transaction characteristics can't be changed while a transaction is in progress"))
        .verify();
    sharedConn.rollbackTransaction().block();
  }

  @Test
  void multipleAutocommit() throws Exception {
    MariadbConnection connection = factory.create().block();
    multipleAutocommit(connection);
    connection.close().block();

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().allowPipelining(false).build())
            .create()
            .block();
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
                    && throwable.getMessage().contains("Connection is close. Cannot send anything"))
        .verify();
  }

  private void consume(Connection connection) {
    int loop = 100;
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

  @Test
  void multiThreading() throws Throwable {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));

    AtomicInteger completed = new AtomicInteger(0);
    ThreadPoolExecutor scheduler =
        new ThreadPoolExecutor(10, 20, 50, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    for (int i = 0; i < 100; i++) {
      scheduler.execute(new ExecuteQueries(completed));
    }
    scheduler.shutdown();
    scheduler.awaitTermination(120, TimeUnit.SECONDS);
    Assertions.assertEquals(100, completed.get());
  }

  @Test
  void multiThreadingSameConnection() throws Throwable {
    // disableLog();
    try {
      AtomicInteger completed = new AtomicInteger(0);
      ThreadPoolExecutor scheduler =
          new ThreadPoolExecutor(10, 20, 50, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

      MariadbConnection connection = factory.create().block();

      for (int i = 0; i < 100; i++) {
        scheduler.execute(new ExecuteQueriesOnSameConnection(completed, connection));
      }
      scheduler.shutdown();
      scheduler.awaitTermination(120, TimeUnit.SECONDS);
      connection.close().block();
      Assertions.assertEquals(100, completed.get());
    } finally {
      Thread.sleep(100);
      // reInitLog();
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
    connection.close().block();
  }

  @Test
  void sessionVariables() throws Exception {
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

    Map<String, String> sessionVariables = new HashMap<>();
    sessionVariables.put("net_read_timeout", "60");
    sessionVariables.put("wait_timeout", "2147483");

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionVariables).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT @@wait_timeout, @@net_read_timeout")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Assertions.assertEquals(row.get(0, BigInteger.class).intValue(), 2147483);
                      Assertions.assertEquals(row.get(1, BigInteger.class).intValue(), 60);
                      Assertions.assertNotEquals(
                          row.get(0, BigInteger.class).intValue(), res[0].intValue());
                      Assertions.assertNotEquals(
                          row.get(1, BigInteger.class).intValue(), res[1].intValue());
                      return 0;
                    }))
        .blockLast();

    connection.close().block();
  }

  protected class ExecuteQueries implements Runnable {
    private final AtomicInteger i;

    public ExecuteQueries(AtomicInteger i) {
      this.i = i;
    }

    public void run() {
      MariadbConnection connection = null;
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
        if (connection != null) connection.close().block();
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
        BigInteger val =
            Flux.from(statement.execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, BigInteger.class)))
                .blockFirst();
        if (rnd != val.intValue())
          throw new IllegalStateException("ERROR rnd:" + rnd + " different to val:" + val);
        i.incrementAndGet();
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
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
    assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> stmt.execute().blockLast(),
        "Connection is close. Cannot send anything");

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().allowPipelining(false).build())
            .create()
            .block();
    commitTransaction(connection);
    MariadbStatement stmt2 = connection.createStatement("DO 1");
    connection.close().block();
    assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> stmt2.execute().blockLast(),
        "Connection is close. Cannot send anything");
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
        "Connection is close. Cannot send anything");

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
        "Connection is close. Cannot send anything");
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
    assertThrows(
        Exception.class,
        () -> sharedConn.rollbackTransactionToSavepoint("wrong").block(),
        "SAVEPOINT wrong does not exist");
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

  IsolationLevel[] levels =
      new IsolationLevel[] {
        IsolationLevel.READ_UNCOMMITTED,
        IsolationLevel.READ_COMMITTED,
        IsolationLevel.SERIALIZABLE,
        IsolationLevel.REPEATABLE_READ
      };

  @Test
  public void isolationLevel() {
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();

    Assertions.assertThrows(
        Exception.class, () -> connection.setTransactionIsolationLevel(null).block());
    for (IsolationLevel level : levels) {
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
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv")));

    BigInteger maxConn =
        sharedConn
            .createStatement("select @@max_connections")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, BigInteger.class)))
            .blockLast();
    Assumptions.assumeTrue(maxConn.intValue() < 600);

    R2dbcTransientResourceException expected = null;
    Mono<?>[] cons = new Mono<?>[maxConn.intValue()];
    for (int i = 0; i < maxConn.intValue(); i++) {
      cons[i] = new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create();
    }
    MariadbConnection[] connections = new MariadbConnection[maxConn.intValue()];
    for (int i = 0; i < maxConn.intValue(); i++) {
      try {
        connections[i] = (MariadbConnection) cons[i].block();
      } catch (R2dbcTransientResourceException e) {
        expected = e;
      }
    }

    for (int i = 0; i < maxConn.intValue(); i++) {
      if (connections[i] != null) {
        connections[i].close().block();
      }
    }
    Assertions.assertNotNull(expected);
    Assertions.assertTrue(expected.getMessage().contains("Too many connections"));
    Thread.sleep(1000);
  }

  @Test
  void killedConnection() {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnection connection = factory.create().block();
    long threadId = connection.getThreadId();
    assertNotNull(connection.getHost());
    assertEquals(TestConfiguration.defaultBuilder.build().getPort(), connection.getPort());
    connection.getPort();
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
    assertTimeout(
        Duration.ofSeconds(2),
        () -> {
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
                t.getMessage().contains("Connection is close. Cannot send anything")
                    || t.getMessage().contains("Connection unexpectedly closed")
                    || t.getMessage().contains("Connection unexpected error"),
                "real msg:" + t.getMessage());
            connection
                .validate(ValidationDepth.LOCAL)
                .as(StepVerifier::create)
                .expectNext(Boolean.FALSE)
                .verifyComplete();
            // reInitLog();
          }
        });
    connection
        .validate(ValidationDepth.LOCAL)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
  }

  @Test
  public void queryTimeout() throws Throwable {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.clone().build())
            .create()
            .block();
    connection.setStatementTimeout(Duration.ofMillis(0500)).block();

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
                      "Query execution was interrupted, maximum statement execution time exceeded"));
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
      if (thread.getName().contains("mariadb")) hasMariaDbThreads = true;
    }
    assertTrue(hasMariaDbThreads);

    connection.close().block();
  }
}

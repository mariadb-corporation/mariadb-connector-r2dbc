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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbResult;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ConnectionTest extends BaseConnectionTest {
  private Level initialReactorLvl;
  private Level initialLvl;

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

    disableLog();
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
      reInitLog();
    }
  }

  @Test
  void multipleCommandStack() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    disableLog();
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
      reInitLog();
      proxy.forceClose();
    }
  }

  @Test
  void connectionWithoutErrorOnClose() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    disableLog();
    MariadbConnection connection = createProxyCon();
    proxy.stop();
    connection.close().block();
    reInitLog();
  }

  @Test
  void connectionDuringError() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    disableLog();
    MariadbConnection connection = createProxyCon();
    new Timer()
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                proxy.stop();
              }
            },
            1000);

    try {
      connection
          .createStatement(
              "select * from information_schema.columns as c1, "
                  + "information_schema.tables, information_schema.tables as t2")
          .execute()
          .flatMap(
              r ->
                  r.map(
                      (rows, meta) -> {
                        return "";
                      }))
          .blockLast();
      Assertions.fail("must have throw exception");
    } catch (Throwable t) {
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getClass());
      Assertions.assertTrue(
          t.getMessage().contains("Connection is close. Cannot send anything")
              || t.getMessage().contains("Connection unexpectedly closed")
              || t.getMessage().contains("Connection unexpected error"),
          "real msg:" + t.getMessage());
      reInitLog();
    }
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
  void socketTimeoutTimeout() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().socketTimeout(Duration.ofSeconds(1)).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    connection.close().block();
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
    connection.beginTransaction().subscribe();
    connection.beginTransaction().block();
    connection.beginTransaction().block();
    connection.close().block();
  }

  @Test
  void multipleAutocommit() throws Exception {
    MariadbConnection connection = factory.create().block();
    connection.setAutoCommit(true).subscribe();
    connection.setAutoCommit(true).block();
    connection.setAutoCommit(false).block();
    connection.close().block();
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
    disableLog();
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
      reInitLog();
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
                      Assertions.assertFalse(row.get(0, BigInteger.class).equals(res[0]));
                      Assertions.assertFalse(row.get(1, BigInteger.class).equals(res[1]));
                      return 0;
                    }))
        .blockLast();

    connection.close().block();
  }

  protected class ExecuteQueries implements Runnable {
    private AtomicInteger i;

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
    private AtomicInteger i;
    private MariadbConnection connection;

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

      if ("skysql".equals(System.getenv("srv")) || "skysql-ha".equals(System.getenv("srv"))) {
        defaultValue = IsolationLevel.READ_COMMITTED;
      }

      Assertions.assertEquals(defaultValue, connection.getTransactionIsolationLevel());
      connection.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED).block();
      Assertions.assertEquals(
          IsolationLevel.READ_UNCOMMITTED, connection.getTransactionIsolationLevel());
      connection.setTransactionIsolationLevel(defaultValue).block();
    } finally {
      connection.close().block();
    }
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
  }

  @Test
  void commitTransaction() {
    sharedConn.createStatement("DROP TABLE IF EXISTS commitTransaction").execute().blockLast();
    sharedConn
        .createStatement("CREATE TABLE commitTransaction (t1 VARCHAR(256))")
        .execute()
        .blockLast();
    sharedConn.setAutoCommit(false).block();
    sharedConn.commitTransaction().block(); // must not do anything
    sharedConn.createStatement("INSERT INTO commitTransaction VALUES ('a')").execute().blockLast();
    sharedConn.commitTransaction().block();
    sharedConn
        .createStatement("SELECT * FROM commitTransaction")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("a"))
        .verifyComplete();
    sharedConn.createStatement("DROP TABLE IF EXISTS commitTransaction").execute().blockLast();
    sharedConn.setAutoCommit(true).block();
  }

  @Test
  void useTransaction() {
    sharedConn.createStatement("DROP TABLE IF EXISTS useTransaction").execute().blockLast();
    sharedConn
        .createStatement("CREATE TABLE useTransaction (t1 VARCHAR(256))")
        .execute()
        .blockLast();
    sharedConn.beginTransaction().block();
    sharedConn.createStatement("INSERT INTO useTransaction VALUES ('a')").execute().blockLast();
    sharedConn.commitTransaction().block();
    sharedConn
        .createStatement("SELECT * FROM useTransaction")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("a"))
        .verifyComplete();
    sharedConn.createStatement("DROP TABLE IF EXISTS useTransaction").execute().blockLast();
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
                .contains(", isolationLevel=IsolationLevel{sql='REPEATABLE READ'}}"));
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
  public void initialIsolationLevel() {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    for (IsolationLevel level : levels) {
      sharedConn
          .createStatement("SET GLOBAL TRANSACTION ISOLATION LEVEL " + level.asSql())
          .execute()
          .blockLast();
      MariadbConnection connection =
          new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();
      assertEquals(level, connection.getTransactionIsolationLevel());
      connection.close().block();
    }

    sharedConn
        .createStatement(
            "SET GLOBAL TRANSACTION ISOLATION LEVEL " + IsolationLevel.REPEATABLE_READ.asSql())
        .execute()
        .blockLast();
  }

  @Test
  public void errorOnConnection() {
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
  }

  @Test
  void killedConnection() {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnection connection = factory.create().block();
    long threadId = connection.getThreadId();

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

    assertThrows(
        R2dbcNonTransientResourceException.class,
        () ->
            connection
                .createStatement(
                    "select * from information_schema.columns as c1, "
                        + "information_schema.tables, "
                        + "information_schema.tables as t2")
                .execute()
                .blockLast(),
        "Connection unexpectedly closed");
  }
}

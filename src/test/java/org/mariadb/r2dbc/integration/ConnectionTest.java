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
import java.math.BigInteger;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class ConnectionTest extends BaseConnectionTest {

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
    }
  }

  @Test
  void connectionDuringError() throws Exception {
    MariadbConnection connection = createProxyCon();
    new java.util.Timer()
        .schedule(
            new java.util.TimerTask() {
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
  void basicConnectionWithoutPipeline() throws Exception {
    MariadbConnectionConfiguration noPipeline =
        TestConfiguration.defaultBuilder.clone().allowPipelining(false).build();
    MariadbConnection connection = new MariadbConnectionFactory(noPipeline).create().block();
    connection.createStatement("SELECT 5")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(5)
        .verifyComplete();
    connection.close().block();
  }


  @Test
  void basicConnectionWithSessionVariable() throws Exception {
    Map<String, String> sessionVariable = new HashMap<>();
    sessionVariable.put("collation_connection", "utf8_slovenian_ci");
    sessionVariable.put("wait_timeout", "3600");
    MariadbConnectionConfiguration cnf =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionVariable).build();
    MariadbConnection connection = new MariadbConnectionFactory(cnf).create().block();
    connection.createStatement("SELECT 5")
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
      Assertions.assertEquals(IllegalArgumentException.class, t.getCause().getClass());
      Assertions.assertTrue(t.getCause().getMessage().contains("Session variable 'test' has no value"));

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

  private void consume(io.r2dbc.spi.Connection connection) {
    int loop = 100;
    int numberOfUserCol = 41;
    io.r2dbc.spi.Statement statement =
        connection.createStatement("select * FROM mysql.user LIMIT 1");

    Flux<Object[]> lastOne;
    lastOne = stat(statement, numberOfUserCol);
    while (loop-- > 0) {
      lastOne = lastOne.thenMany(stat(statement, numberOfUserCol));
    }
    Object[] obj = lastOne.blockLast();
  }

  private Flux<Object[]> stat(io.r2dbc.spi.Statement statement, int numberOfUserCol) {
    return Flux.from(statement.execute())
        .flatMap(
            it ->
                it.map(
                    (row, rowMetadata) -> {
                      Object[] objs = new Object[numberOfUserCol];
                      for (int i = 0; i < numberOfUserCol; i++) {
                        objs[i] = row.get(i);
                      }
                      return objs;
                    }));
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
        io.r2dbc.spi.Statement statement = connection.createStatement("select " + rnd);
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
        io.r2dbc.spi.Statement statement = connection.createStatement("select " + rnd);
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
    Assertions.assertEquals(
        IsolationLevel.REPEATABLE_READ, sharedConn.getTransactionIsolationLevel());
    sharedConn.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED).block();
    Assertions.assertEquals(
        IsolationLevel.READ_UNCOMMITTED, sharedConn.getTransactionIsolationLevel());
    sharedConn.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ).block();
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
    Assertions.assertTrue(
        sharedConn
                .toString()
                .contains(
                    "MariadbConnection{client=Client{isClosed=false, "
                        + "context=ConnectionContext{")
            && sharedConn
                .toString()
                .contains(", isolationLevel=IsolationLevel{sql='REPEATABLE READ'}}"));
  }
}

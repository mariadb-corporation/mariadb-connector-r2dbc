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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ConnectionTest extends BaseTest {

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
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      return row.get(0, String.class);
                    }));
    Mono.from(connection.close()).block();
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
}

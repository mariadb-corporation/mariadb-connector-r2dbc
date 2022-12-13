// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.spi.*;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.tools.TcpProxy;
import org.mariadb.r2dbc.util.HostAddress;
import reactor.test.StepVerifier;

public class FailoverConnectionTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TABLE IF NOT EXISTS sequence_0_to_999 (t1 int)")
        .execute()
        .blockLast();
    if (sharedConn
            .createStatement("SELECT COUNT(*) FROM sequence_0_to_999")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
            .blockLast()
        != 1000) {
      sharedConn.createStatement("TRUNCATE TABLE sequence_0_to_999").execute().blockLast();
      if (isMariaDBServer()) {
        sharedConn
            .createStatement("INSERT INTO sequence_0_to_999 SELECT * from seq_0_to_999")
            .execute()
            .blockLast();
      } else {
        MariadbStatement stmt =
            sharedConn.createStatement("INSERT INTO sequence_0_to_999 VALUES (?)");
        stmt.bind(0, 0);
        for (int i = 1; i < 1000; i++) {
          stmt.add();
          stmt.bind(0, i);
        }
        stmt.execute().blockLast();
      }
    }
  }

  @Test
  @Timeout(20)
  void multipleCommandStack() throws Exception {
    Assumptions.assumeFalse(System.getenv("TRAVIS") != null && isWindows);
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    MariadbConnection connection = createFailoverProxyConnection(HaMode.SEQUENTIAL, false, false);
    try {
      connection.createStatement("SET @con=1").execute().blockLast();
      assertTrue(connection.validate(ValidationDepth.REMOTE).block());
      proxy.restart(5000);
      Thread.sleep(200);
      assertTrue(connection.validate(ValidationDepth.REMOTE).block());
      Thread.sleep(200);

      assertTrue(connection.validate(ValidationDepth.REMOTE).block());
      connection.close().block();
    } finally {
      proxy.forceClose();
      Thread.sleep(50);
    }
  }

  @Test
  @Timeout(5)
  void transactionReplayFalse() throws Exception {
    Assumptions.assumeFalse(System.getenv("TRAVIS") != null && isWindows);
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    MariadbConnection connection = createFailoverProxyConnection(HaMode.SEQUENTIAL, false, false);
    try {
      connection.setAutoCommit(false).block();
      connection.beginTransaction().block();
      connection.createStatement("SET @con=1").execute().blockLast();

      proxy.restart();
      connection
          .createStatement("SELECT @con")
          .execute()
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && throwable.getMessage().contains("In progress transaction was lost"))
          .verify();
    } finally {
      proxy.forceClose();
      Thread.sleep(50);
    }
  }

  @Test
  @Timeout(50)
  void transactionReplayFailingBetweenCmds() throws Exception {
    Assumptions.assumeFalse(System.getenv("TRAVIS") != null && isWindows);
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    try {
      transactionReplayFailingBetweenCmds(
          createFailoverProxyConnection(HaMode.SEQUENTIAL, true, false));
      transactionReplayFailingBetweenCmds(
          createFailoverProxyConnection(HaMode.SEQUENTIAL, true, true));
    } finally {
      Thread.sleep(50);
    }
  }

  private void transactionReplayFailingBetweenCmds(MariadbConnection connection) throws Exception {
    try {
      connection.createStatement("SET @con=1").execute().blockLast();

      //      proxy.restartForce();

      Optional<Object> res =
          connection
              .createStatement("SELECT @con")
              .execute()
              .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
              .blockLast();
      assertTrue(res.isPresent());
      assertEquals(1L, res.get());

      connection.createStatement("DROP TABLE IF EXISTS testReplay").execute().blockLast();
      connection.createStatement("CREATE TABLE testReplay(id INT)").execute().blockLast();
      connection.createStatement("INSERT INTO testReplay VALUE (1)").execute().blockLast();
      connection.setAutoCommit(false).block();
      connection.beginTransaction().block();

      connection.createStatement("INSERT INTO testReplay VALUE (2)").execute().blockLast();
      connection
          .createStatement("INSERT INTO testReplay VALUE (?)")
          .bind(0, 3)
          .execute()
          .blockLast();

      connection
          .createStatement("INSERT INTO testReplay VALUE (?)")
          .bind(0, 4)
          .execute()
          .blockLast();

      proxy.restart();

      connection
          .createStatement("INSERT INTO testReplay VALUE (?)")
          .bind(0, 5)
          .execute()
          .blockLast();
      connection.commitTransaction().block();
      connection
          .createStatement("SELECT id from testReplay")
          .execute()
          .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
          .as(StepVerifier::create)
          .expectNext(1, 2, 3, 4, 5)
          .verifyComplete();
      connection.createStatement("DROP TABLE IF EXISTS testReplay").execute().blockLast();

    } finally {
      connection.close().block();
      proxy.forceClose();
      Thread.sleep(100);
    }
  }

  @Test
  @Timeout(60)
  void transactionReplayFailingDuringCmd() throws Exception {
    Assumptions.assumeFalse(System.getenv("TRAVIS") != null && isWindows);
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    transactionReplayFailingDuringCmd(
        createFailoverProxyConnection(HaMode.SEQUENTIAL, true, false));
    transactionReplayFailingDuringCmd(createFailoverProxyConnection(HaMode.SEQUENTIAL, true, true));
  }

  private void transactionReplayFailingDuringCmd(MariadbConnection connection) throws Exception {
    connection.setAutoCommit(false).block();
    connection.beginTransaction().block();

    AtomicInteger expectedResult = new AtomicInteger(0);
    AtomicReference<Throwable> resultingError = new AtomicReference<>();
    connection
        .createStatement(
            "SELECT * from sequence_0_to_999 as tab1, sequence_0_to_999 tab2 order by tab2.t1 ASC, tab1.t1 ASC")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      int i = row.get(0, Integer.class);
                      assertEquals(expectedResult.getAndIncrement() % 1000, i);
                      return i;
                    }))
        .doOnError(t -> resultingError.set(t))
        .subscribe();

    while (expectedResult.get() == 0) Thread.sleep(0, 10000);
    proxy.restart();

    int maxTime = 100;
    while (maxTime-- > 0) {
      if (expectedResult.get() >= 1_000_000) break;
      if (resultingError.get() != null) {

        resultingError.get().printStackTrace();
        assertTrue(
            resultingError
                    .get()
                    .getMessage()
                    .contains(
                        "Driver has reconnect connection after a communications link failure with")
                && resultingError.get().getMessage().contains("during command."));
        break;
      }
      Thread.sleep(100);
    }

    connection.close().block();
    proxy.forceClose();
    Thread.sleep(50);
  }

  private MariadbConnection createFailoverProxyConnection(
      HaMode haMode, boolean transactionReplay, boolean usePrepare) throws Exception {

    HostAddress hostAddress = TestConfiguration.defaultConf.getHostAddresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.getHost(), hostAddress.getPort());
    } catch (IOException i) {
      throw new Exception("proxy error", i);
    }

    List<HostAddress> hosts = new ArrayList<>();
    hosts.add(new HostAddress("localhost", 9999));
    hosts.add(new HostAddress("localhost", proxy.getLocalPort()));
    MariadbConnectionConfiguration.Builder builder =
        TestConfiguration.defaultBuilder
            .clone()
            .haMode(haMode.name())
            .transactionReplay(transactionReplay)
            .connectTimeout(Duration.ofSeconds(5))
            .useServerPrepStmts(usePrepare)
            .hostAddresses(hosts)
            .host(System.getenv("TRAVIS") != null ? hostAddress.getHost() : "localhost");

    if (TestConfiguration.defaultConf
        .getSslConfig()
        .getSslMode()
        .equals(org.mariadb.jdbc.export.SslMode.VERIFY_FULL)) {
      builder.sslMode(SslMode.VERIFY_CA);
    }

    MariadbConnectionConfiguration confProxy = builder.build();

    return new MariadbConnectionFactory(confProxy).create().block();
  }
}

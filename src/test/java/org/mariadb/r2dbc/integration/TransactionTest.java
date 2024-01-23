// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TransactionTest extends BaseConnectionTest {
  private static final String insertCmd =
      "INSERT INTO `users` (`first_name`, `last_name`, `email`) VALUES ('MariaDB', 'Row',"
          + " 'mariadb@test.com')";

  @BeforeAll
  public static void before2() {
    drop();
    sharedConn
        .createStatement(
            "CREATE TABLE `users` (\n"
                + " `id` int(11) NOT NULL AUTO_INCREMENT,\n"
                + " `first_name` varchar(255) NOT NULL,\n"
                + " `last_name` varchar(255) NOT NULL,\n"
                + " `email` varchar(255) NOT NULL,\n"
                + " PRIMARY KEY (`id`)\n"
                + ")")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void drop() {
    sharedConn.createStatement("DROP TABLE IF EXISTS `users`").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE `users`").execute().blockLast();
  }

  @Test
  void commit() {
    MariadbConnection conn = factory.create().block();
    conn.beginTransaction()
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(Flux.from(conn.commitTransaction()).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 1);
    conn.close().block();
  }

  @Test
  void multipleBegin() {
    MariadbConnection conn = factory.create().block();
    // must issue only one begin command
    conn.beginTransaction().thenMany(conn.beginTransaction()).blockLast();
    conn.beginTransaction().block();
    conn.close().block();
  }

  @Test
  void commitWithoutTransaction() {
    // must issue no commit command
    MariadbConnection conn = factory.create().block();
    conn.commitTransaction().thenMany(conn.commitTransaction()).blockLast();
    conn.commitTransaction().block();
    conn.close().block();
  }

  @Test
  void rollbackWithoutTransaction() {
    // must issue no commit command
    MariadbConnection conn = factory.create().block();
    conn.rollbackTransaction().thenMany(conn.rollbackTransaction()).blockLast();
    conn.rollbackTransaction().block();
    conn.close().block();
  }

  @Test
  void createSavepoint() {
    // must issue multiple savepoints
    MariadbConnection conn = factory.create().block();
    conn.createSavepoint("t").thenMany(conn.createSavepoint("t2")).blockLast();
    conn.createSavepoint("t3").block();
    conn.close().block();
  }

  @Test
  void rollback() {
    MariadbConnection conn = factory.create().block();
    conn.beginTransaction()
        .thenMany(conn.createStatement(insertCmd).execute())
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    conn.rollbackTransaction().block();
    checkInserted(conn, 0);
    conn.close().block();
  }

  @Test
  void rollbackPipelining() {
    MariadbConnection conn = factory.create().block();
    conn.beginTransaction()
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 0);
    conn.close().block();
  }

  @Test
  void releaseSavepoint() throws Exception {
    MariadbConnection conn = factory.create().block();
    conn.setAutoCommit(false).block();
    conn.createStatement(insertCmd)
        .execute()
        .thenMany(conn.createSavepoint("mySavePoint1"))
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(Flux.from(conn.releaseSavepoint("mySavePoint1")).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 2);
    conn.rollbackTransaction().block();
    conn.setAutoCommit(true).block();
    conn.close().block();
  }

  @Test
  void rollbackSavepoint() {
    MariadbConnection conn = factory.create().block();
    conn.setAutoCommit(false).block();
    conn.createStatement(insertCmd)
        .execute()
        .thenMany(conn.createSavepoint("mySavePoint2"))
        .thenMany(conn.createStatement(insertCmd).execute())
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    conn.rollbackTransactionToSavepoint("mySavePoint2").block();
    checkInserted(conn, 1);
    conn.rollbackTransaction().block();
    conn.setAutoCommit(true).block();
    conn.close().block();
  }

  @Test
  void rollbackSavepointPipelining() {
    MariadbConnection conn = factory.create().block();
    conn.setAutoCommit(false).block();
    conn.createStatement(insertCmd)
        .execute()
        .thenMany(conn.createSavepoint("mySavePoint3"))
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(
            Flux.from(conn.rollbackTransactionToSavepoint("mySavePoint3")).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 1);
    conn.rollbackTransaction().block();
    conn.setAutoCommit(true).block();
    conn.close().block();
  }

  private void checkInserted(MariadbConnection conn, int expectedValue) {
    Integer res =
        conn.createStatement("SELECT count(*) FROM `users`")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
            .blockLast();
    Assertions.assertEquals(expectedValue, res);
  }
}

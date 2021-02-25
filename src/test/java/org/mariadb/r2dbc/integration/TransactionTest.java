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
import java.util.*;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class TransactionTest extends BaseConnectionTest {
  private static String insertCmd =
      "INSERT INTO `users` (`first_name`, `last_name`, `email`) VALUES ('MariaDB', 'Row', 'mariadb@test.com')";

  @BeforeAll
  public static void before2() {
    drop();
    sharedConn
        .createStatement(
            "CREATE TABLE `users` (\n"
                + " `id` int(11) NOT NULL AUTO_INCREMENT,\n"
                + " `first_name` varchar(255) COLLATE utf16_slovak_ci NOT NULL,\n"
                + " `last_name` varchar(255) COLLATE utf16_slovak_ci NOT NULL,\n"
                + " `email` varchar(255) COLLATE utf16_slovak_ci NOT NULL,\n"
                + " PRIMARY KEY (`id`)\n"
                + ")")
        .execute()
        .blockLast();
  }

  @BeforeEach
  public void beforeEch() {
    sharedConn.createStatement("TRUNCATE TABLE `users`").execute().blockLast();
  }

  @AfterAll
  public static void drop() {
    sharedConn.createStatement("DROP TABLE IF EXISTS `users`").execute().blockLast();
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
    conn.close();
  }

  @Test
  void multipleBegin() {
    MariadbConnection conn = factory.create().block();
    // must issue only one begin command
    conn.beginTransaction().thenMany(conn.beginTransaction()).blockLast();
    conn.beginTransaction().block();
    conn.close();
  }

  @Test
  void commitWithoutTransaction() {
    // must issue no commit command
    sharedConn.commitTransaction().thenMany(sharedConn.commitTransaction()).blockLast();
    sharedConn.commitTransaction().block();
  }

  @Test
  void rollbackWithoutTransaction() {
    // must issue no commit command
    sharedConn.rollbackTransaction().thenMany(sharedConn.rollbackTransaction()).blockLast();
    sharedConn.rollbackTransaction().block();
  }

  @Test
  void createSavepoint() {
    // must issue multiple savepoints
    sharedConn.createSavepoint("t").thenMany(sharedConn.createSavepoint("t")).blockLast();
    sharedConn.createSavepoint("t").block();
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
    conn.close();
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
    conn.close();
  }

  @Test
  void releaseSavepoint() {
    MariadbConnection conn = factory.create().block();
    conn.setAutoCommit(false).block();
    conn.createStatement(insertCmd)
        .execute()
        .thenMany(conn.createSavepoint("mySavePoint"))
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(Flux.from(conn.releaseSavepoint("mySavePoint")).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 2);
    conn.rollbackTransaction().block();
    conn.close();
  }

  @Test
  void rollbackSavepoint() {
    MariadbConnection conn = factory.create().block();
    conn.setAutoCommit(false).block();
    conn.createStatement(insertCmd)
        .execute()
        .thenMany(conn.createSavepoint("mySavePoint"))
        .thenMany(conn.createStatement(insertCmd).execute())
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    conn.rollbackTransactionToSavepoint("mySavePoint").block();
    checkInserted(conn, 1);
    conn.rollbackTransaction().block();
    conn.close();
  }

  @Test
  void rollbackSavepointPipelining() {
    MariadbConnection conn = factory.create().block();
    conn.setAutoCommit(false).block();
    conn.createStatement(insertCmd)
        .execute()
        .thenMany(conn.createSavepoint("mySavePoint"))
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(
            Flux.from(conn.rollbackTransactionToSavepoint("mySavePoint")).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 1);
    conn.rollbackTransaction().block();
    conn.close();
  }

  private Mono<Void> checkInserted(MariadbConnection conn, int expectedValue) {
    conn.createStatement("SELECT count(*) FROM `users`")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(expectedValue)
        .verifyComplete();
    return Mono.empty();
  }
}

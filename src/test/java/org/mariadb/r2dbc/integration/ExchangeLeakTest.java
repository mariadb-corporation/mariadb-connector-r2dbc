// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import reactor.test.StepVerifier;

public class ExchangeLeakTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS exchange_leak_test").execute().blockLast();
    sharedConn
        .createStatement("CREATE TABLE exchange_leak_test (id INT PRIMARY KEY, value VARCHAR(100))")
        .execute()
        .blockLast();

    for (int i = 0; i < 100; i++) {
      sharedConn
          .createStatement("INSERT INTO exchange_leak_test VALUES (?, ?)")
          .bind(0, i)
          .bind(1, "value_" + i)
          .execute()
          .blockLast();
    }
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS exchange_leak_test").execute().blockLast();
  }

  @Test
  void cancelledSubscriptionShouldNotLeakExchange() {
    AtomicInteger cancelledQueryCount = new AtomicInteger(0);

    StepVerifier.create(
            sharedConn
                .createStatement("SELECT * FROM exchange_leak_test ORDER BY id")
                .execute()
                .flatMap(
                    result ->
                        result.map(
                            (row, metadata) -> {
                              cancelledQueryCount.incrementAndGet();
                              return row.get(0, Integer.class);
                            }))
                .take(1))
        .expectNextCount(1)
        .verifyComplete();

    assertEquals(1, cancelledQueryCount.get());

    for (int i = 0; i < 5; i++) {
      AtomicInteger count = new AtomicInteger(0);

      StepVerifier.create(
              sharedConn
                  .createStatement("SELECT COUNT(*) as cnt FROM exchange_leak_test")
                  .execute()
                  .flatMap(
                      result ->
                          result.map(
                              (row, metadata) -> {
                                count.incrementAndGet();
                                return row.get("cnt", Long.class);
                              }))
                  .timeout(Duration.ofSeconds(5)))
          .expectNext(100L)
          .verifyComplete();

      assertEquals(1, count.get(), "Query " + (i + 1) + " should complete successfully");
    }
  }

  @Test
  void immediateCancellationShouldNotBlockSubsequentQueries() {
    for (int i = 0; i < 3; i++) {
      StepVerifier.create(
              sharedConn
                  .createStatement("SELECT * FROM exchange_leak_test ORDER BY id")
                  .execute()
                  .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                  .take(0))
          .verifyComplete();
    }

    StepVerifier.create(
            sharedConn
                .createStatement("SELECT COUNT(*) as cnt FROM exchange_leak_test")
                .execute()
                .flatMap(result -> result.map((row, metadata) -> row.get("cnt", Long.class)))
                .timeout(Duration.ofSeconds(5)))
        .expectNext(100L)
        .verifyComplete();
  }

  @Test
  void multipleRapidCancellationsShouldNotBlockConnection() {
    for (int i = 0; i < 10; i++) {
      StepVerifier.create(
              sharedConn
                  .createStatement("SELECT * FROM exchange_leak_test ORDER BY id")
                  .execute()
                  .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                  .take(1))
          .expectNextCount(1)
          .verifyComplete();
    }

    StepVerifier.create(
            sharedConn
                .createStatement("SELECT COUNT(*) as cnt FROM exchange_leak_test")
                .execute()
                .flatMap(result -> result.map((row, metadata) -> row.get("cnt", Long.class))))
        .expectNext(100L)
        .verifyComplete();
  }

  @Test
  void cancelledSubscriptionWithTimeoutShouldNotDeadlock() {
    StepVerifier.create(
            sharedConn
                .createStatement("SELECT * FROM exchange_leak_test ORDER BY id")
                .execute()
                .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                .take(1)
                .timeout(Duration.ofSeconds(5)))
        .expectNextCount(1)
        .verifyComplete();

    StepVerifier.create(
            sharedConn
                .createStatement("SELECT 1")
                .execute()
                .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                .timeout(Duration.ofSeconds(5)))
        .expectNext(1)
        .verifyComplete();
  }

  @Test
  void interleavedCancelledAndCompletedQueriesShouldWork() {
    for (int i = 0; i < 5; i++) {
      StepVerifier.create(
              sharedConn
                  .createStatement("SELECT * FROM exchange_leak_test ORDER BY id")
                  .execute()
                  .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                  .take(1))
          .expectNextCount(1)
          .verifyComplete();

      StepVerifier.create(
              sharedConn
                  .createStatement("SELECT COUNT(*) as cnt FROM exchange_leak_test")
                  .execute()
                  .flatMap(result -> result.map((row, metadata) -> row.get("cnt", Long.class))))
          .expectNext(100L)
          .verifyComplete();
    }
  }

  @Test
  void clientAbortBeforeOnRequestShouldNotLeakExchange() throws Exception {
    // Test with 1ns timeout - may timeout OR complete depending on system performance
    // Both outcomes are acceptable as long as connection doesn't leak
    for (int i = 0; i < 10; i++) {
      StepVerifier.create(
              sharedConn
                  .createStatement("SELECT * FROM exchange_leak_test ORDER BY id")
                  .execute()
                  .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                  .timeout(Duration.ofNanos(1))
                  .onErrorResume(
                      java.util.concurrent.TimeoutException.class,
                      e ->
                          reactor.core.publisher.Flux
                              .empty())) // Convert timeout to empty completion
          .thenConsumeWhile(x -> true) // Consume any data if it arrives
          .verifyComplete(); // Either completes with data or empty after timeout
    }

    // Verify connection is not leaked and subsequent queries work
    StepVerifier.create(
            sharedConn
                .createStatement("SELECT COUNT(*) as cnt FROM exchange_leak_test")
                .execute()
                .flatMap(result -> result.map((row, metadata) -> row.get("cnt", Long.class)))
                .timeout(Duration.ofSeconds(5)))
        .expectNext(100L)
        .verifyComplete();
  }

  @Test
  void asyncQueriesWithCancellationShouldNotBlockQueue() throws Exception {
    StepVerifier.create(
            sharedConn
                .createStatement("SELECT * FROM exchange_leak_test ORDER BY id")
                .execute()
                .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                .take(1))
        .expectNextCount(1)
        .verifyComplete();

    StepVerifier.create(
            sharedConn
                .createStatement("SELECT * FROM exchange_leak_test ORDER BY id")
                .execute()
                .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                .take(0))
        .verifyComplete();

    StepVerifier.create(
            sharedConn
                .createStatement("SELECT * FROM exchange_leak_test ORDER BY id")
                .execute()
                .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                .take(1))
        .expectNextCount(1)
        .verifyComplete();

    StepVerifier.create(
            sharedConn
                .createStatement("SELECT COUNT(*) as cnt FROM exchange_leak_test")
                .execute()
                .flatMap(result -> result.map((row, metadata) -> row.get("cnt", Long.class)))
                .timeout(Duration.ofSeconds(5)))
        .expectNext(100L)
        .verifyComplete();

    for (int i = 0; i < 3; i++) {
      StepVerifier.create(
              sharedConn
                  .createStatement("SELECT " + i)
                  .execute()
                  .flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                  .timeout(Duration.ofSeconds(5)))
          .expectNext(i)
          .verifyComplete();
    }
  }
}

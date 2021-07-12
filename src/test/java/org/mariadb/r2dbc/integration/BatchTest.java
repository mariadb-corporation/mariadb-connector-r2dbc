// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbBatch;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class BatchTest extends BaseConnectionTest {

  @Test
  void basicBatch() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE basicBatch (id int, test varchar(10))")
        .execute()
        .blockLast();
    MariadbBatch batch = sharedConn.createBatch();
    int[] res = new int[20];
    for (int i = 0; i < 20; i++) {
      batch.add("INSERT INTO basicBatch VALUES (" + i + ", 'test" + i + "')");
      res[i] = i;
    }

    batch
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(1, 1, 1, 1, 1)
        .expectNextCount(15)
        .then(
            () -> {
              sharedConn
                  .createStatement("SELECT id FROM basicBatch")
                  .execute()
                  .flatMap(r -> r.map((row, metadata) -> row.get(0)))
                  .as(StepVerifier::create)
                  .expectNext(0, 1, 2, 3, 4)
                  .expectNextCount(15)
                  .verifyComplete();
            })
        .verifyComplete();
  }

  @Test
  void multiQueriesBatch() throws Exception {
    MariadbConnectionConfiguration confMulti =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(true).build();
    MariadbConnection multiConn = new MariadbConnectionFactory(confMulti).create().block();
    multiConn
        .createStatement("CREATE TEMPORARY TABLE multiBatch (id int, test varchar(10))")
        .execute()
        .blockLast();
    MariadbBatch batch = multiConn.createBatch();
    int[] res = new int[20];
    for (int i = 0; i < 20; i++) {
      batch.add("INSERT INTO multiBatch VALUES (" + i + ", 'test" + i + "')");
      res[i] = i;
    }

    batch
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(1, 1, 1, 1, 1)
        .expectNextCount(15)
        .then(
            () -> {
              multiConn
                  .createStatement("SELECT id FROM multiBatch")
                  .execute()
                  .flatMap(r -> r.map((row, metadata) -> row.get(0)))
                  .as(StepVerifier::create)
                  .expectNext(0, 1, 2, 3, 4)
                  .expectNextCount(15)
                  .verifyComplete();
              multiConn.close().block();
            })
        .verifyComplete();
  }

  @Test
  void noMultiQueriesBatch() throws Exception {
    MariadbConnectionConfiguration confMulti =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(false).build();
    MariadbConnection multiConn = new MariadbConnectionFactory(confMulti).create().block();
    multiConn
        .createStatement("CREATE TEMPORARY TABLE multiBatch (id int, test varchar(10))")
        .execute()
        .blockLast();
    MariadbBatch batch = multiConn.createBatch();
    int[] res = new int[20];
    for (int i = 0; i < 20; i++) {
      batch.add("INSERT INTO multiBatch VALUES (" + i + ", 'test" + i + "')");
      res[i] = i;
    }

    batch
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(1, 1, 1, 1, 1)
        .expectNextCount(15)
        .then(
            () -> {
              multiConn
                  .createStatement("SELECT id FROM multiBatch")
                  .execute()
                  .flatMap(r -> r.map((row, metadata) -> row.get(0)))
                  .as(StepVerifier::create)
                  .expectNext(0, 1, 2, 3, 4)
                  .expectNextCount(15)
                  .verifyComplete();
              multiConn.close().block();
            })
        .verifyComplete();
  }

  @Test
  void batchWithParameter() {
    MariadbBatch batch = sharedConn.createBatch();
    batch.add("INSERT INTO JJ VALUES ('g?')");
    batch.add("INSERT INTO JJ VALUES ('g') /* ?*/");
    batch.add("INSERT INTO JJ VALUES ('g') /* :named_param*/");
    try {
      batch.add("INSERT INTO JJ VALUES (?)");
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage().contains("Statement with parameters cannot be batched (sql:'"));
    }
    try {
      batch.add("INSERT INTO JJ VALUES (:named_param)");
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage().contains("Statement with parameters cannot be batched (sql:'"));
    }
  }

  @Test
  void batchError() {
    batchError(sharedConn);
    batchError(sharedConnPrepare);
  }

  void batchError(MariadbConnection conn) {
    conn.createStatement("CREATE TEMPORARY TABLE basicBatch2 (id int, test varchar(10))")
        .execute()
        .blockLast();
    conn.createStatement("INSERT INTO basicBatch2 VALUES (?, ?)")
        .bind(0, 1)
        .bind(1, "dd")
        .execute()
        .blockLast();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            conn.createStatement("INSERT INTO basicBatch2 VALUES (?, ?)")
                .bind(0, 1)
                .bind(1, "dd")
                .add()
                .bind(1, "dd")
                .add(),
        "Parameter at position 0 is not set");
  }
}

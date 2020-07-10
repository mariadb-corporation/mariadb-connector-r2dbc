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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbBatch;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class BatchTest extends BaseTest {

  @Test
  void basicBatch() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE basicBatch (id int, test varchar(10))")
        .execute()
        .blockLast();
    MariadbBatch batch = sharedConn.createBatch();
    int[] res = new int[100];
    for (int i = 0; i < 100; i++) {
      batch.add("INSERT INTO basicBatch VALUES (" + i + ", 'test" + i + "')");
      res[i] = i;
    }

    batch
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(1, 1, 1, 1, 1)
        .expectNextCount(95)
        .verifyComplete();
    sharedConn
        .createStatement("SELECT id FROM basicBatch")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNext(0, 1, 2, 3, 4)
        .expectNextCount(95)
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
    int[] res = new int[100];
    for (int i = 0; i < 100; i++) {
      batch.add("INSERT INTO multiBatch VALUES (" + i + ", 'test" + i + "')");
      res[i] = i;
    }

    batch
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(1, 1, 1, 1, 1)
        .expectNextCount(95)
        .verifyComplete();
    multiConn
        .createStatement("SELECT id FROM multiBatch")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNext(0, 1, 2, 3, 4)
        .expectNextCount(95)
        .verifyComplete();
    multiConn.close().block();
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
}

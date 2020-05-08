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

import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class StatementBatchingTest extends BaseTest {

  @Test
  void batchStatement() {
    batchStatement(sharedConn);
  }

  @Test
  void batchStatementPrepare() {
    batchStatement(sharedConnPrepare);
  }

  void batchStatement(MariadbConnection connection) {
    connection
        .createStatement(
            "CREATE TEMPORARY TABLE batchStatement (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    connection
        .createStatement("INSERT INTO batchStatement values (?, ?)")
        .bind(0, 1)
        .bind(1, "test")
        .add()
        .bind(1, "test2")
        .bind(0, 2)
        .execute()
        .blockLast();

    connection
        .createStatement("SELECT * FROM batchStatement")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
        .as(StepVerifier::create)
        .expectNext("1test", "2test2")
        .verifyComplete();
  }

  @Test
  void batchStatementResultSet() {
    batchStatementResultSet(sharedConn);
  }

  @Test
  void batchStatementResultSetPrepare() {
    batchStatementResultSet(sharedConnPrepare);
  }

  void batchStatementResultSet(MariadbConnection connection) {
    connection
        .createStatement(
            "CREATE TEMPORARY TABLE batchStatementResultSet (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();
    connection
        .createStatement("INSERT INTO batchStatementResultSet values (1, 'test1'), (2, 'test2')")
        .execute()
        .blockLast();
    connection
        .createStatement("SELECT test FROM batchStatementResultSet WHERE id = ?")
        .bind(0, 1)
        .add()
        .bind(0, 2)
        .add()
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("test1", "test2", "test1")
        .verifyComplete();
  }
}

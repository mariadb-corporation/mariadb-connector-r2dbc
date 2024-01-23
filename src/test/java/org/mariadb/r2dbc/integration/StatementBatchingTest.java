// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class StatementBatchingTest extends BaseConnectionTest {

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
            "CREATE TEMPORARY TABLE batchStatement (id int not null primary key auto_increment,"
                + " test varchar(10))")
        .execute()
        .blockLast();
    connection.beginTransaction().block();
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
    connection.rollbackTransaction().block();
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
            "CREATE TEMPORARY TABLE batchStatementResultSet (id int not null primary key"
                + " auto_increment, test varchar(10))")
        .execute()
        .blockLast();
    connection.beginTransaction().block();
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
    connection.rollbackTransaction().block();
  }
}

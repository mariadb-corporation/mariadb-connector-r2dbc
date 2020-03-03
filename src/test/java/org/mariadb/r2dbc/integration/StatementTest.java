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

import io.r2dbc.spi.R2dbcTransientResourceException;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.test.StepVerifier;

public class StatementTest extends BaseTest {

  @Test
  void bindOnStatementWithoutParameter() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (1,2)");
    try {
      stmt.bind(0, 1);
      Assertions.fail("must have thrown exception");
    } catch (UnsupportedOperationException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains(
                  "Binding parameters is not supported for the statement 'INSERT INTO someTable values (1,2)'"));
    }

    try {
      stmt.bind("name", 1);
      Assertions.fail("must have thrown exception");
    } catch (UnsupportedOperationException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains(
                  "Binding parameters is not supported for the statement 'INSERT INTO someTable values (1,2)'"));
    }
    try {
      stmt.bindNull(0, String.class);
      Assertions.fail("must have thrown exception");
    } catch (UnsupportedOperationException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains(
                  "Binding parameters is not supported for the statement 'INSERT INTO someTable values (1,2)'"));
    }
    try {
      stmt.bindNull("name", String.class);
      Assertions.fail("must have thrown exception");
    } catch (UnsupportedOperationException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains(
                  "Binding parameters is not supported for the statement 'INSERT INTO someTable values (1,2)'"));
    }
  }

  @Test
  void bindOnPreparedStatementWrongParameter() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (?, ?)");
    try {
      stmt.bind(-1, 1);
      Assertions.fail("must have thrown exception");
    } catch (IndexOutOfBoundsException e) {
      Assertions.assertTrue(e.getMessage().contains("index must be in 0-1 range but value is -1"));
    }
    try {
      stmt.bind(2, 1);
      Assertions.fail("must have thrown exception");
    } catch (IndexOutOfBoundsException e) {
      Assertions.assertTrue(e.getMessage().contains("index must be in 0-1 range but value is 2"));
    }

    try {
      stmt.bindNull(-1, String.class);
      Assertions.fail("must have thrown exception");
    } catch (IndexOutOfBoundsException e) {
      Assertions.assertTrue(e.getMessage().contains("index must be in 0-1 range but value is -1"));
    }
    try {
      stmt.bindNull(2, String.class);
      Assertions.fail("must have thrown exception");
    } catch (IndexOutOfBoundsException e) {
      Assertions.assertTrue(e.getMessage().contains("index must be in 0-1 range but value is 2"));
    }
  }

  @Test
  void bindWrongName() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (:name1, :name2)");
    try {
      stmt.bind(null, 1);
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(e.getMessage().contains("identifier cannot be null"));
    }
    try {
      stmt.bind("other", 1);
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains("No parameter with name 'other' found (possible values [name1, name2])"));
    }
    try {
      stmt.bindNull("other", String.class);
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains("No parameter with name 'other' found (possible values [name1, name2])"));
    }
  }

  @Test
  void bindUnknownClass() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (?)");
    try {
      stmt.bind(0, sharedConn);
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains(
                  "No encoder for class org.mariadb.r2dbc.MariadbConnection (parameter at index 0)"));
    }
  }

  @Test
  void bindOnPreparedStatementWithoutAllParameter() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (?, ?)");
    stmt.bind(1, 1);

    try {
      stmt.execute();
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(e.getMessage().contains("Parameter at position 0 is not set"));
    }
  }

  @Test
  void statementToString() {
    String st = sharedConn.createStatement("SELECT 1").toString();
    Assertions.assertTrue(
        st.contains("MariadbSimpleQueryStatement{") && st.contains("sql='SELECT 1'"));
    String st2 = sharedConn.createStatement("SELECT ?").toString();
    Assertions.assertTrue(
        st2.contains("MariadbClientParameterizedQueryStatement{")
            && st2.contains("sql='SELECT ?'"));
  }

  @Test
  void fetchSize() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    // sequence table requirement
    Assumptions.assumeTrue(meta.isMariaDBServer() && minVersion(10, 1, 0));

    sharedConn
        .createStatement("SELECT * FROM seq_1_to_1000")
        .fetchSize(100)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNextCount(1000)
        .verifyComplete();
    sharedConn
        .createStatement("SELECT * FROM seq_1_to_1000 WHERE seq > ?")
        .fetchSize(100)
        .bind(0, 800)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNextCount(200)
        .verifyComplete();
  }

  @Test
  public void returning() {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 5, 1));

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE INSERT_RETURNING (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    sharedConn
        .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES ('test1'), ('test2')")
        .returnGeneratedValues("id", "test")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
        .as(StepVerifier::create)
        .expectNext("1test1", "2test2")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES ('test3'), ('test4')")
        .returnGeneratedValues("id")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("3", "4")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES ('a'), ('b')")
        .returnGeneratedValues()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
        .as(StepVerifier::create)
        .expectNext("5a", "6b")
        .verifyComplete();
  }

  @Test
  public void returningBefore105() {
    Assumptions.assumeFalse((isMariaDBServer() && minVersion(10, 5, 1)));

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE returningBefore105 (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    try {
      sharedConn
          .createStatement("INSERT INTO returningBefore105(test) VALUES ('test1'), ('test2')")
          .returnGeneratedValues("id", "test")
          .execute();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains("returnGeneratedValues can have only one column before MariaDB 10.5.1"));
    }

    sharedConn
        .createStatement("INSERT INTO returningBefore105(test) VALUES ('test1'), ('test2')")
        .returnGeneratedValues("id")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000")
                    && ((throwable.getMessage().contains("Connector cannot get generated ID"))))
        .verify();

    sharedConn
        .createStatement("INSERT INTO returningBefore105(test) VALUES ('test1')")
        .returnGeneratedValues("id")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("3")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO returningBefore105(test) VALUES ('test3')")
        .returnGeneratedValues("TEST_COL_NAME")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("TEST_COL_NAME", String.class)))
        .as(StepVerifier::create)
        .expectNext("4")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO returningBefore105(test) VALUES ('a')")
        .returnGeneratedValues()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", String.class)))
        .as(StepVerifier::create)
        .expectNext("5")
        .verifyComplete();
  }

  @Test
  public void prepareReturning() {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 5, 1));

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE prepareReturning (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    sharedConn
        .createStatement("INSERT INTO prepareReturning(test) VALUES (?), (?)")
        .returnGeneratedValues("id", "test")
        .bind(0, "test1")
        .bind(1, "test2")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
        .as(StepVerifier::create)
        .expectNext("1test1", "2test2")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO prepareReturning(test) VALUES (?), (?)")
        .returnGeneratedValues("id")
        .bind(0, "test3")
        .bind(1, "test4")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("3", "4")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO prepareReturning(test) VALUES (?), (?)")
        .returnGeneratedValues()
        .bind(0, "a")
        .bind(1, "b")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
        .as(StepVerifier::create)
        .expectNext("5a", "6b")
        .verifyComplete();
  }

  @Test
  public void prepareReturningBefore105() {
    Assumptions.assumeFalse((isMariaDBServer() && minVersion(10, 5, 1)));

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE prepareReturningBefore105 (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    try {
      sharedConn
          .createStatement("INSERT INTO prepareReturningBefore105(test) VALUES (?), (?)")
          .bind(0, "test1")
          .bind(1, "test2")
          .returnGeneratedValues("id", "test")
          .execute();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains("returnGeneratedValues can have only one column before MariaDB 10.5.1"));
    }

    sharedConn
        .createStatement("INSERT INTO prepareReturningBefore105(test) VALUES (?), (?)")
        .bind(0, "test1")
        .bind(1, "test2")
        .returnGeneratedValues("id")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000")
                    && ((throwable.getMessage().contains("Connector cannot get generated ID"))))
        .verify();

    sharedConn
        .createStatement("INSERT INTO prepareReturningBefore105(test) VALUES (?)")
        .bind(0, "test1")
        .returnGeneratedValues("id")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("3")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO prepareReturningBefore105(test) VALUES (?)")
        .bind(0, "test1")
        .returnGeneratedValues("TEST_COL_NAME")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("TEST_COL_NAME", String.class)))
        .as(StepVerifier::create)
        .expectNext("4")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO prepareReturningBefore105(test) VALUES (?)")
        .bind(0, "a")
        .returnGeneratedValues()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", String.class)))
        .as(StepVerifier::create)
        .expectNext("5")
        .verifyComplete();
  }
}

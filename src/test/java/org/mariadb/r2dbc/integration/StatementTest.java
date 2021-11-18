// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import io.r2dbc.spi.Statement;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.test.StepVerifier;

public class StatementTest extends BaseConnectionTest {

  @Test
  void bindOnStatementWithoutParameter() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (1,2)");
    try {
      stmt = stmt.add(); // mean nothing there
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
  void bindOnNamedParameterStatement() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (:1,:2)");

    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bind("bla", "nok"),
        "No parameter with name 'bla' found (possible values [1, 2])");
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bindNull("bla", String.class),
        "No parameter with name 'bla' found (possible values [1, 2])");
    stmt.bind("1", "ok").bindNull("2", String.class);
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
    MariadbStatement stmt = sharedConn.createStatement("INSERT INTO someTable values (?)");
    try {
      stmt.bind(0, sharedConn).execute().subscribe();
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains(
                  "No encoder for class org.mariadb.r2dbc.MariadbConnection (parameter at index 0)"));
    }
  }

  @Test
  void wrongSql() {
    assertThrows(
        IllegalArgumentException.class,
        () -> sharedConn.createStatement(null),
        "sql must not be null");
    assertThrows(
        IllegalArgumentException.class,
        () -> sharedConn.createStatement(""),
        "Statement cannot be empty");
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
  public void dupplicate() {
    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE dupplicate (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    sharedConn
        .createStatement("INSERT INTO dupplicate(test) VALUES ('test1'), ('test2')")
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(2)
        .verifyComplete();
    if (isMariaDBServer() && minVersion(10, 5, 1)) {
      sharedConn
          .createStatement("INSERT INTO dupplicate(test) VALUES ('test3'), ('test4') RETURNING *")
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectNext(2)
          .verifyComplete();

      sharedConn
          .createStatement("INSERT INTO dupplicate(test) VALUES ('test5') RETURNING *")
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectNext(1)
          .verifyComplete();
    }

    sharedConn
        .createStatement("INSERT INTO dupplicate(id, test) VALUES (1, 'dupplicate')")
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcDataIntegrityViolationException
                    && (throwable.getMessage().contains("Duplicate entry '1' for key") ||
                        throwable.getMessage().contains("Duplicate key in container")))
        .verify();
  }

  @Test
  public void getPosition() {
    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE get_pos (t1 varchar(10), t2 varchar(10), t3 varchar(10), t4 varchar(10))")
        .execute()
        .blockLast();

    sharedConn
        .createStatement("INSERT INTO get_pos VALUES ('test1', 'test2', 'test3', 'test4')")
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(1)
        .verifyComplete();

    sharedConn
        .createStatement("SELECT * FROM get_pos")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Assertions.assertEquals("test4", row.get(3, String.class));
                      Assertions.assertEquals("test3", row.get(2, String.class));
                      Assertions.assertEquals("test2", row.get(1, String.class));
                      return row.get(0, String.class);
                    }))
        .as(StepVerifier::create)
        .expectNext("test1")
        .verifyComplete();
  }

  @Test
  public void returning() {
    Assumptions.assumeTrue(isMariaDBServer());

    if (!minVersion(10, 5, 1)) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              sharedConn
                  .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES ('test1'), ('test2')")
                  .returnGeneratedValues("id", "test"));
    }

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE INSERT_RETURNING (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    sharedConn
        .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES ('test3')")
        .returnGeneratedValues("id")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("1")
        .verifyComplete();

    if (minVersion(10, 5, 1)) {

      sharedConn
          .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES ('test3'), ('test4')")
          .returnGeneratedValues("id")
          .execute()
          .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
          .as(StepVerifier::create)
          .expectNext("2", "3")
          .verifyComplete();
      sharedConn
          .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES ('test1'), ('test2')")
          .returnGeneratedValues("id", "test")
          .execute()
          .flatMap(
              r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
          .as(StepVerifier::create)
          .expectNext("4test1", "5test2")
          .verifyComplete();

      sharedConn
          .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES ('a'), ('b')")
          .returnGeneratedValues()
          .execute()
          .flatMap(
              r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
          .as(StepVerifier::create)
          .expectNext("6a", "7b")
          .verifyComplete();
    }
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
  public void returningBefore105WithParameter() {
    Assumptions.assumeFalse((isMariaDBServer() && minVersion(10, 5, 1)));

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE returningBefore105WithParameter (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    try {
      sharedConn
          .createStatement("INSERT INTO returningBefore105WithParameter(test) VALUES (?), (?)")
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
        .createStatement("INSERT INTO returningBefore105WithParameter(test) VALUES (?), (?)")
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
        .createStatement("INSERT INTO returningBefore105WithParameter(test) VALUES (?)")
        .bind(0, "test1")
        .returnGeneratedValues("id")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("3")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO returningBefore105WithParameter(test) VALUES (?)")
        .bind(0, "test3")
        .returnGeneratedValues("TEST_COL_NAME")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("TEST_COL_NAME", String.class)))
        .as(StepVerifier::create)
        .expectNext("4")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO returningBefore105WithParameter(test) VALUES (?)")
        .bind(0, "a")
        .returnGeneratedValues()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", String.class)))
        .as(StepVerifier::create)
        .expectNext("5")
        .verifyComplete();

    sharedConn
        .createStatement("INSERT INTO returningBefore105WithParameter(test) VALUES (?)")
        .returnGeneratedValues()
        .bind(0, "b")
        .add()
        .bind(0, "c")
        .add()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", String.class)))
        .as(StepVerifier::create)
        .expectNext("6", "7")
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

    sharedConn
        .createStatement("INSERT INTO prepareReturning(test) VALUES (?)")
        .returnGeneratedValues()
        .bind(0, "c")
        .add()
        .bind(0, "d")
        .add()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", String.class)))
        .as(StepVerifier::create)
        .expectNext("7", "8")
        .verifyComplete();
  }

  @Test
  void parameterNull() {
    parameterNull(sharedConn);
    parameterNull(sharedConnPrepare);
  }

  void parameterNull(MariadbConnection conn) {
    conn.createStatement("CREATE TEMPORARY TABLE parameterNull(t varchar(10), t2 varchar(10))")
        .execute()
        .blockLast();
    conn.createStatement("INSERT INTO parameterNull VALUES ('1', '1'), (null, '2'), (null, null)")
        .execute()
        .blockLast();
    MariadbStatement stmt =
        conn.createStatement("SELECT t2 FROM parameterNull WHERE COALESCE(t,?) is null");
    stmt.bindNull(0, Integer.class)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("2"), Optional.empty())
        .verifyComplete();
    stmt.bindNull(0, String.class)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("2"), Optional.empty())
        .verifyComplete();

    stmt.bindNull(0, this.getClass())
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("2"), Optional.empty())
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

  @Test
  public void generatedId() {

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE generatedId (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    sharedConn
        .createStatement("INSERT INTO generatedId(test) VALUES (?)")
        .bind(0, "test0")
        .returnGeneratedValues()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", Integer.class)))
        .as(StepVerifier::create)
        .expectNext(1)
        .verifyComplete();

    sharedConn
        .createStatement("ALTER TABLE generatedId AUTO_INCREMENT = 60000")
        .execute()
        .blockLast();

    sharedConn
        .createStatement("INSERT INTO generatedId(test) VALUES (?)")
        .bind(0, "test1")
        .returnGeneratedValues()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", Integer.class)))
        .as(StepVerifier::create)
        .expectNext(60000)
        .verifyComplete();

    sharedConn
        .createStatement("ALTER TABLE generatedId AUTO_INCREMENT = 70000")
        .execute()
        .blockLast();

    sharedConn
        .createStatement("INSERT INTO generatedId(test) VALUES (?)")
        .bind(0, "test1")
        .returnGeneratedValues()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", Integer.class)))
        .as(StepVerifier::create)
        .expectNext(70000)
        .verifyComplete();

    sharedConn
        .createStatement("ALTER TABLE generatedId AUTO_INCREMENT = 20000000")
        .execute()
        .blockLast();

    sharedConn
        .createStatement("INSERT INTO generatedId(test) VALUES (?)")
        .bind(0, "test1")
        .returnGeneratedValues()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", Integer.class)))
        .as(StepVerifier::create)
        .expectNext(20000000)
        .verifyComplete();
  }
}

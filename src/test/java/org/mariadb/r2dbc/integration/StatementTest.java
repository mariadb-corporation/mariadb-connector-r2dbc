// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import io.r2dbc.spi.Statement;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class StatementTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() throws Exception {
    dropAll();
    sharedConn
        .createStatement("CREATE TABLE parameterNull(t varchar(10), t2 varchar(10))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE prepareReturningBefore105 (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE returningBefore105WithParameter (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    sharedConn
        .createStatement(
            "CREATE TABLE generatedId (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE prepareReturning (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE dupplicate (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE get_pos (t1 varchar(10), t2 varchar(10), t3 varchar(10), t4 varchar(10))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE INSERT_RETURNING (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    sharedConn
        .createStatement(
            "CREATE TABLE returningBefore105 (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void dropAll() {
    sharedConn.createStatement("DROP TABLE IF EXISTS parameterNull").execute().blockLast();
    sharedConn
        .createStatement("DROP TABLE IF EXISTS prepareReturningBefore105")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("DROP TABLE IF EXISTS returningBefore105WithParameter")
        .execute()
        .blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS generatedId").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS prepareReturning").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS dupplicate").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS get_pos").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS INSERT_RETURNING").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS returningBefore105").execute().blockLast();
  }

  @Test
  void bindOnStatementWithoutParameter() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (1,2)");
    assertThrowsContains(
        IndexOutOfBoundsException.class,
        () -> stmt.bind(1, 1),
        "Binding index 1 when only 0 parameters are expected");

    assertThrowsContains(
        IndexOutOfBoundsException.class,
        () -> stmt.bind("name", 1),
        "Binding parameters is not supported for the statement 'INSERT INTO someTable values (1,2)'");
    assertThrowsContains(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(0, String.class),
        "Binding parameters is not supported for the statement 'INSERT INTO someTable values (1,2)'");
    assertThrowsContains(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull("name", String.class),
        "Binding parameters is not supported for the statement 'INSERT INTO someTable values (1,2)'");
  }

  @Test
  void bindOnNamedParameterStatement() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (:1,:2)");

    assertThrows(
        NoSuchElementException.class,
        () -> stmt.bind("bla", "nok"),
        "No parameter with name 'bla' found (possible values [1, 2])");
    assertThrows(
        NoSuchElementException.class,
        () -> stmt.bindNull("bla", String.class),
        "No parameter with name 'bla' found (possible values [1, 2])");
    stmt.bind("1", "ok").bindNull("2", String.class);
  }

  @Test
  void bindOnPreparedStatementWrongParameter() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (?, ?)");
    assertThrowsContains(
        IndexOutOfBoundsException.class,
        () -> stmt.bind(-1, 1),
        "wrong index value -1, index must be positive");
    assertThrowsContains(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(-1, String.class),
        "wrong index value -1, index must be positive");
    assertThrowsContains(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(2, String.class),
        "Cannot bind parameter 2, statement has 2 parameters");
  }

  @Test
  void bindWrongName() {
    Statement stmt = sharedConn.createStatement("INSERT INTO someTable values (:name1,:name2)");
    try {
      stmt.bind(null, 1);
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(e.getMessage().contains("identifier cannot be null"));
    }
    try {
      stmt.bind("other", 1);
      Assertions.fail("must have thrown exception");
    } catch (NoSuchElementException e) {
      Assertions.assertTrue(
          e.getMessage()
              .contains("No parameter with name 'other' found (possible values [name1, name2])"));
    }
    try {
      stmt.bindNull("other", String.class);
      Assertions.fail("must have thrown exception");
    } catch (NoSuchElementException e) {
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
    } catch (IllegalStateException e) {
      Assertions.assertTrue(e.getMessage().contains("Parameter at position 0 is not set"));
    }
  }

  @Test
  void statementToString() {
    String st = sharedConn.createStatement("SELECT 1").toString();
    Assertions.assertTrue(
        st.contains("MariadbClientParameterizedQueryStatement{") && st.contains("sql='SELECT 1'"),
        st);
    String st2 = sharedConn.createStatement("SELECT ?").toString();
    Assertions.assertTrue(
        st2.contains("MariadbClientParameterizedQueryStatement{") && st2.contains("sql='SELECT ?'"),
        st2);
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
  void metadataNotSkipped() {
    // issue with maxscale 22.08.2
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));

    String sql;
    int param = 500;
    StringBuilder sb = new StringBuilder("select ?");
    for (int i = 1; i < param; i++) {
      sb.append(",?");
    }
    sql = sb.toString();
    int[] rnds = randParams();
    io.r2dbc.spi.Statement statement = sharedConnPrepare.createStatement(sql);
    for (int j = 0; j < 2; j++) {
      for (int i = 0; i < param; i++) {
        statement.bind(i, rnds[i]);
      }

      Integer val =
          Flux.from(statement.execute())
              .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
              .blockLast();
      if (rnds[0] != val) {
        System.out.println("not Expected");
        Assertions.fail("ERROR");
      }
    }
  }

  private static int[] randParams() {
    int[] rnds = new int[1000];
    for (int i = 0; i < 1000; i++) {
      rnds[i] = (int) (Math.random() * 1000);
    }
    return rnds;
  }

  @Test
  public void dupplicate() {
    sharedConn.beginTransaction().block();
    sharedConn
        .createStatement("INSERT INTO dupplicate(test) VALUES ('test1'), ('test2')")
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(2L)
        .verifyComplete();
    if (isMariaDBServer() && minVersion(10, 5, 1)) {
      sharedConn
          .createStatement("INSERT INTO dupplicate(test) VALUES ('test3'), ('test4') RETURNING *")
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectNext(2L)
          .verifyComplete();

      sharedConn
          .createStatement("INSERT INTO dupplicate(test) VALUES ('test5') RETURNING *")
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectNext(1L)
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
                    && (throwable.getMessage().contains("Duplicate entry '1' for key")
                        || throwable.getMessage().contains("Duplicate key in container")))
        .verify();
    sharedConn.rollbackTransaction().block();
  }

  @Test
  public void sinkEndCheck() throws Throwable {
    Assumptions.assumeTrue(isMariaDBServer());
    AtomicReference<Disposable> d = new AtomicReference<>();
    AtomicReference<Disposable> d2 = new AtomicReference<>();
    MariadbConnection connection = factory.create().block();
    connection.beginTransaction().block();
    try {
      Flux<Integer> flux =
          connection
              .createStatement("SELECT * from seq_1_to_10000")
              .execute()
              .flatMap(
                  r ->
                      r.map(
                          (row, metadata) -> {
                            d2.set(connection.createStatement("COMMIT").execute().subscribe());
                            while (d.get() == null) {
                              try {
                                Thread.sleep(50);
                              } catch (InterruptedException i) {
                                i.printStackTrace();
                              }
                            }
                            d.get().dispose();
                            return row.get(0, Integer.class);
                          }));
      d.set(flux.subscribe());
      for (int i = 0; i < 1000; i++) {
        Thread.sleep(20);
        if (d2.get() != null && d2.get().isDisposed()) break;
      }
      Assertions.assertTrue(d2.get() != null && d2.get().isDisposed());
    } finally {
      try {
        connection.close().block();
      } catch (Exception e) {
      }
    }
  }

  @Test
  public void sinkFirstOnly() throws Throwable {
    Assumptions.assumeTrue(isMariaDBServer());
    AtomicReference<Disposable> d2 = new AtomicReference<>();
    MariadbConnection connection = factory.create().block();
    connection.beginTransaction().block();
    try {
      Flux<Integer> flux =
          connection
              .createStatement("SELECT * from seq_1_to_10000")
              .execute()
              .flatMap(
                  r ->
                      r.map(
                          (row, metadata) -> {
                            d2.set(connection.createStatement("COMMIT").execute().subscribe());
                            return row.get(0, Integer.class);
                          }));
      flux.blockFirst();

      for (int i = 0; i < 1000; i++) {
        Thread.sleep(20);
        if (d2.get() != null && d2.get().isDisposed()) break;
      }
      Assertions.assertTrue(d2.get() != null && d2.get().isDisposed());
    } finally {
      try {
        connection.close().block();
      } catch (Exception e) {
      }
    }
  }

  @Test
  public void getPosition() {
    sharedConn.beginTransaction().block();
    sharedConn
        .createStatement("INSERT INTO get_pos VALUES ('test1', 'test2', 'test3', 'test4')")
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(1L)
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
    sharedConn.rollbackTransaction().block();
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
    sharedConn.beginTransaction().block();
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
    sharedConn.rollbackTransaction().block();
  }

  @Test
  public void returningBefore105() {
    Assumptions.assumeFalse((isMariaDBServer() && minVersion(10, 5, 1)));
    sharedConn.beginTransaction().block();
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
    sharedConn.rollbackTransaction().block();
  }

  @Test
  public void returningBefore105WithParameter() {
    Assumptions.assumeFalse((isMariaDBServer() && minVersion(10, 5, 1)));
    sharedConn.beginTransaction().block();
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
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", String.class)))
        .as(StepVerifier::create)
        .expectNext("6", "7")
        .verifyComplete();
    sharedConn.rollbackTransaction().block();
  }

  @Test
  public void prepareReturning() {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 5, 1));
    sharedConn.beginTransaction().block();
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
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", String.class)))
        .as(StepVerifier::create)
        .expectNext("7", "8")
        .verifyComplete();

    assertThrows(
        IllegalStateException.class,
        () ->
            sharedConn
                .createStatement("INSERT INTO prepareReturning(test) VALUES (?)")
                .returnGeneratedValues()
                .bind(0, "c")
                .add()
                .bind(0, "d")
                .add()
                .execute(),
        "Parameter at position 0 is not set");
    sharedConn.rollbackTransaction().block();
  }

  @Test
  void parameterNull() {
    parameterNull(sharedConn);
    parameterNull(sharedConnPrepare);
  }

  void parameterNull(MariadbConnection conn) {
    conn.beginTransaction().block();
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

    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bindNull(0, this.getClass()),
        "No encoder for class org.mariadb.r2dbc.integration.StatementTest");
    conn.rollbackTransaction().block();
  }

  @Test
  public void prepareReturningBefore105() {
    Assumptions.assumeFalse((isMariaDBServer() && minVersion(10, 5, 1)));
    sharedConn.beginTransaction().block();
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
    sharedConn.rollbackTransaction().block();
  }

  @Test
  public void generatedId() {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    sharedConn.beginTransaction().block();
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
    sharedConn.rollbackTransaction().block();
  }
}

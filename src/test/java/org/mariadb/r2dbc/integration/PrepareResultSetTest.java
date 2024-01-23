// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.R2dbcTransientResourceException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.test.StepVerifier;

public class PrepareResultSetTest extends BaseConnectionTest {
  private static final List<String> stringList =
      Arrays.asList(
          "456",
          "789000002",
          "25",
          "30",
          "456.45",
          "127",
          "2020",
          "45",
          "ዩኒኮድ ወረጘ የጝ",
          "65445681355454",
          "987456",
          "45000",
          "45.9",
          "-2",
          "2045",
          "12",
          "ዩኒኮድ What does this means ?");

  @BeforeAll
  public static void before2() {
    after2();
    sharedConn
        .createStatement(
            "CREATE TABLE PrepareResultSetTest("
                + "id int, "
                + "i2 BIGINT, "
                + "i3 int, "
                + "i4 MEDIUMINT, "
                + "i5 FLOAT, "
                + "i6 SMALLINT, "
                + "i7 YEAR, "
                + "i8 TINYINT, "
                + "i9 VARCHAR(256), "
                + "i10 BIGINT, "
                + "i11 int, "
                + "i12 MEDIUMINT, "
                + "i13 FLOAT, "
                + "i14 SMALLINT, "
                + "i15 YEAR, "
                + "i16 TINYINT, "
                + "i17 VARCHAR(256)) DEFAULT CHARSET=utf8mb4")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO PrepareResultSetTest VALUES (456,789000002,25,30, 456.45,127,2020,45,'ዩኒኮድ"
                + " ወረጘ የጝ',65445681355454,987456,45000, 45.9, -2, 2045, 12, 'ዩኒኮድ What does this"
                + " means ?')")
        .execute()
        .blockLast();
    sharedConn.createStatement("CREATE TABLE myTable(a varchar(10))").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE parameterLengthEncoded(t0 VARCHAR(1024),t1 MEDIUMTEXT) DEFAULT"
                + " CHARSET=utf8mb4")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE parameterLengthEncodedLong (t0 LONGTEXT) DEFAULT CHARSET=utf8mb4")
        .execute()
        .blockLast();
    sharedConn.createStatement("CREATE TABLE validateParam(t0 VARCHAR(10))").execute().blockLast();
    sharedConnPrepare
        .createStatement(
            "CREATE TABLE missingParameter(t1 VARCHAR(256),t2 VARCHAR(256)) DEFAULT"
                + " CHARSET=utf8mb4")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS PrepareResultSetTest").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS myTable").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS parameterLengthEncoded").execute().blockLast();
    sharedConn
        .createStatement("DROP TABLE IF EXISTS parameterLengthEncodedLong")
        .execute()
        .blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS validateParam").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS missingParameter").execute().blockLast();
  }

  @Test
  void bindWithName() {
    sharedConnPrepare
        .createStatement("INSERT INTO myTable (a) VALUES (:var1)")
        .bind("var1", "test")
        .execute();
  }

  @Test
  void validateParam() {
    // disabling with maxscale due to MXS-3956
    // to be re-enabled when > 6.1.1
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Assertions.assertThrows(
        Exception.class,
        () ->
            sharedConnPrepare
                .createStatement("INSERT INTO validateParam (t0) VALUES (?)")
                .execute()
                .flatMap(r -> r.getRowsUpdated())
                .blockLast());
    assertThrows(
        Exception.class,
        () ->
            sharedConnPrepare
                .createStatement("INSERT INTO validateParam (t0) VALUES (?)")
                .execute()
                .flatMap(r -> r.getRowsUpdated())
                .blockLast(),
        "Parameter at position 0 is not set");
  }

  @Test
  void parameterLengthEncoded() {
    Assumptions.assumeTrue(maxAllowedPacket() >= 17 * 1024 * 1024);
    Assumptions.assumeTrue(
        !sharedConn.getMetadata().getDatabaseVersion().contains("maxScale-6.1.")
            && !"maxscale".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(runLongTest());
    char[] arr1024 = new char[1024];
    for (int i = 0; i < arr1024.length; i++) {
      arr1024[i] = (char) ('a' + (i % 10));
    }
    char[] arr = new char[16_000_000];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = (char) ('a' + (i % 10));
    }
    char[] arr2 = new char[17_000_000];
    for (int i = 0; i < arr2.length; i++) {
      arr2[i] = (char) ('a' + (i % 10));
    }
    String arr1024St = String.valueOf(arr1024);
    String arrSt = String.valueOf(arr);
    String arrSt2 = String.valueOf(arr2);
    sharedConnPrepare.beginTransaction().block();
    sharedConnPrepare
        .createStatement("INSERT INTO parameterLengthEncoded VALUES (?, ?)")
        .bind(0, arr1024St)
        .bind(1, arrSt)
        .add()
        .bind(0, arr1024St)
        .bind(1, arrSt2)
        .execute()
        .blockLast();
    AtomicBoolean first = new AtomicBoolean(true);
    sharedConnPrepare
        .createStatement("SELECT * FROM parameterLengthEncoded WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      String t0 = row.get(0, String.class);
                      if (first.get()) {
                        String t1 = row.get(1, String.class);
                        Assertions.assertEquals(arrSt, t1);
                        first.set(false);
                      } else {
                        String t1 = row.get(1, String.class);
                        Assertions.assertEquals(arrSt2, t1);
                      }
                      Assertions.assertEquals(arr1024St, t0);
                      return t0;
                    }))
        .as(StepVerifier::create)
        .expectNext(String.valueOf(arr1024))
        .verifyComplete();
    first.set(true);
    sharedConnPrepare
        .createStatement("SELECT * FROM parameterLengthEncoded /* ? */ ")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      String t0 = row.get(0, String.class);
                      if (first.get()) {
                        String t1 = row.get(1, String.class);
                        Assertions.assertEquals(arrSt, t1);
                        first.set(false);
                      } else {
                        String t1 = row.get(1, String.class);
                        Assertions.assertEquals(arrSt2, t1);
                      }
                      Assertions.assertEquals(arr1024St, t0);
                      return t0;
                    }))
        .as(StepVerifier::create)
        .expectNext(String.valueOf(arr1024))
        .verifyComplete();
    sharedConnPrepare.rollbackTransaction().block();
  }

  @Test
  void parameterLengthEncodedLong() {
    Assumptions.assumeTrue(maxAllowedPacket() >= 20 * 1024 * 1024 + 500);
    Assumptions.assumeTrue(
        !sharedConn.getMetadata().getDatabaseVersion().contains("maxScale-6.1.")
            && !"maxscale".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    // out of memory on travis and 10.1
    Assumptions.assumeFalse(
        "mariadb:10.1".equals(System.getenv("DB")) || "mysql:5.6".equals(System.getenv("DB")));

    char[] arr = new char[20_000_000];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = (char) ('a' + (i % 10));
    }
    String val = String.valueOf(arr);

    sharedConnPrepare.beginTransaction().block();
    sharedConnPrepare
        .createStatement("INSERT INTO parameterLengthEncodedLong VALUES (?)")
        .bind(0, val)
        .execute()
        .blockLast();
    sharedConnPrepare
        .createStatement("SELECT * FROM parameterLengthEncodedLong")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class).length()))
        .as(StepVerifier::create)
        .expectNext(val.length())
        .verifyComplete();
    sharedConnPrepare.commitTransaction().block();
  }

  @Test
  void missingParameter() {
    // missing first parameter
    sharedConnPrepare.beginTransaction().block();
    MariadbStatement stmt =
        sharedConnPrepare.createStatement("INSERT INTO missingParameter(t1, t2) VALUES (?, ?)");

    assertThrows(
        IllegalStateException.class,
        () -> stmt.bind(1, "test").execute().blockLast(),
        "Parameter at position 0 is not set");
    assertThrows(
        IllegalArgumentException.class, () -> stmt.bind(null, null), "identifier cannot be null");
    assertThrows(
        NoSuchElementException.class,
        () -> stmt.bind("test", null),
        "No parameter with name 'test' found");
    stmt.bindNull(0, null).bind(1, "test").execute().blockLast();

    stmt.bind(1, "test");
    assertThrows(
        IllegalStateException.class, () -> stmt.add(), "Parameter at position 0 is not set");
    sharedConnPrepare
        .createStatement("SELECT * FROM missingParameter")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Assertions.assertNull(row.get(0));
                      return row.get(1, String.class);
                    }))
        .as(StepVerifier::create)
        .expectNext("test")
        .verifyComplete();
    sharedConnPrepare.rollbackTransaction().block();
  }

  @Test
  void resultSetSkippingRes() {
    for (int i = 10; i < 17; i++) {
      int finalI = i;
      sharedConnPrepare
          .createStatement("SELECT * FROM PrepareResultSetTest WHERE 1 = ?")
          .bind(0, 1)
          .execute()
          .flatMap(r -> r.map((row, metadata) -> row.get(finalI, String.class)))
          .as(StepVerifier::create)
          .expectNext(stringList.get(i))
          .verifyComplete();
    }
  }

  @Test
  public void returning() {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 5, 1));

    sharedConnPrepare
        .createStatement(
            "CREATE TEMPORARY TABLE INSERT_RETURNING (id int not null primary key auto_increment,"
                + " test varchar(10))")
        .execute()
        .blockLast();

    MariadbStatement st =
        sharedConnPrepare
            .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES (?), (?)")
            .bind(0, "test1")
            .bind(1, "test2")
            .fetchSize(44)
            .returnGeneratedValues("id", "test");
    Assertions.assertTrue(st.toString().contains("generatedColumns=[id, test]"));
    st.execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class) + row.get(1, String.class)))
        .as(StepVerifier::create)
        .expectNext("1test1", "2test2")
        .verifyComplete();

    sharedConnPrepare
        .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES (?), (?)")
        .returnGeneratedValues("id")
        .bind(0, "test3")
        .bind(1, "test4")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("3", "4")
        .verifyComplete();

    sharedConnPrepare
        .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES (?), (?)")
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
  public void returningBefore105() {
    Assumptions.assumeFalse((isMariaDBServer() && minVersion(10, 5, 1)));

    sharedConnPrepare
        .createStatement(
            "CREATE TEMPORARY TABLE returningBefore105 (id int not null primary key auto_increment,"
                + " test varchar(10))")
        .execute()
        .blockLast();

    try {
      sharedConnPrepare
          .createStatement("INSERT INTO returningBefore105(test) VALUES (?), (?)")
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

    sharedConnPrepare
        .createStatement("INSERT INTO returningBefore105(test) VALUES (?), (?)")
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

    sharedConnPrepare
        .createStatement("INSERT INTO returningBefore105(test) VALUES (?)")
        .bind(0, "test1")
        .returnGeneratedValues("id")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Assertions.assertEquals("id", metadata.getColumnMetadata(0).getName());
                      return row.get(0, String.class);
                    }))
        .as(StepVerifier::create)
        .expectNext("3")
        .verifyComplete();

    sharedConnPrepare
        .createStatement("INSERT INTO returningBefore105(test) VALUES (?)")
        .bind(0, "test3")
        .returnGeneratedValues("TEST_COL_NAME")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Assertions.assertEquals(
                          "TEST_COL_NAME", metadata.getColumnMetadata(0).getName());
                      return row.get("TEST_COL_NAME", String.class);
                    }))
        .as(StepVerifier::create)
        .expectNext("4")
        .verifyComplete();

    sharedConnPrepare
        .createStatement("INSERT INTO returningBefore105(test) VALUES (?)")
        .bind(0, "a")
        .returnGeneratedValues()
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("id", String.class)))
        .as(StepVerifier::create)
        .expectNext("5")
        .verifyComplete();
  }

  @Test
  void parameterVerification() {
    MariadbStatement stmt =
        sharedConn.createStatement("SELECT * FROM PrepareResultSetTest WHERE 1 = ?");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bind(-1, 1),
        "wrong index value -1, index must be positive");
    stmt.bind(0, 1).execute().subscribe().dispose();
    stmt.bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Long.class)))
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bind(2, 1),
        "Binding index 2 when only 1 parameters are expected");
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bind(0, this).execute().blockLast(),
        "No encoder for class org.mariadb.r2dbc.integration.PrepareResultSetTest (parameter at"
            + " index 0) ");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(-1, Integer.class),
        "wrong index value -1, index must be positive");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(2, Integer.class),
        "Cannot bind parameter 2, statement has 1 parameters");
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bindNull(0, this.getClass()),
        "No encoder for class org.mariadb.r2dbc.integration.PrepareResultSetTest");

    // error crashing maxscale 6.1.x
    Assumptions.assumeTrue(
        !sharedConn.getMetadata().getDatabaseVersion().contains("maxScale-6.1.")
            && !"skysql-ha".equals(System.getenv("srv")));

    stmt.bindNull(0, String.class);
    stmt.execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Long.class)))
        .as(StepVerifier::create)
        .expectNextCount(0)
        .verifyComplete();
    // no parameter
    assertThrows(
        IllegalStateException.class,
        () -> stmt.execute().blockLast(),
        "Parameter at position 0 is not set");
    Assertions.assertThrows(IllegalStateException.class, () -> stmt.add());
  }

  @Test
  void cannotPrepare() throws Throwable {
    // unexpected error "unexpected message received when no command was send: 0x48000002"
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeFalse(isXpand());
    MariadbConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder.clone().useServerPrepStmts(true).build();
    MariadbConnection conn = new MariadbConnectionFactory(confPipeline).create().block();
    try {
      assertThrows(
          Exception.class,
          () ->
              conn.createStatement("xa start ?, 'abcdef', 3")
                  .bind(0, "12")
                  .execute()
                  .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
                  .blockLast(),
          "You have an error in your SQL syntax");
    } finally {
      conn.close().block();
    }
  }

  @Test
  void parameterNull() {
    sharedConnPrepare
        .createStatement("CREATE TEMPORARY TABLE parameterNull(t varchar(10), t2 varchar(10))")
        .execute()
        .blockLast();
    sharedConnPrepare.beginTransaction().block();
    sharedConnPrepare
        .createStatement("INSERT INTO parameterNull VALUES ('1', '1'), (null, '2'), (null, null)")
        .execute()
        .blockLast();
    MariadbStatement stmt =
        sharedConnPrepare.createStatement(
            "SELECT t2 FROM parameterNull WHERE COALESCE(t,?) is null");
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
        "No encoder for class org.mariadb.r2dbc.integration.PrepareResultSetTest");
    stmt.bindNull(0, String.class)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("2"), Optional.empty())
        .verifyComplete();
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bindNull(null, String.class),
        "identifier cannot be null");
    assertThrows(
        NoSuchElementException.class,
        () -> stmt.bindNull("fff", String.class),
        "No parameter with name 'fff' found (possible values [null])");
    sharedConnPrepare.rollbackTransaction().block();
  }

  @Test
  void prepareReuse() {
    // https://jira.mariadb.org/browse/XPT-599 XPand doesn't support DO
    Assumptions.assumeFalse(isXpand());
    MariadbStatement stmt = sharedConnPrepare.createStatement("DO 1 = ?");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bind(-1, 1),
        "wrong index value -1, index must be positive");
    stmt.bind(0, 1).execute().blockLast();
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bind(2, 1),
        "Binding index 2 when only 1 parameters are expected");
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bind(0, this).execute().blockLast(),
        "No encoder for class org.mariadb.r2dbc.integration.PrepareResultSetTest (parameter at"
            + " index 0) ");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(-1, Integer.class),
        "wrong index value -1, index must be positive");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(2, Integer.class),
        "Cannot bind parameter 2, statement has 1 parameters");
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bindNull(0, this.getClass()),
        "No encoder for class org.mariadb.r2dbc.integration.PrepareResultSetTest (parameter at"
            + " index 0)");
    stmt.bindNull(0, String.class);
    stmt.bind(0, 1);
    stmt.execute().blockLast();
    // no parameter
    assertThrows(
        IllegalStateException.class,
        () -> stmt.execute().blockLast(),
        "Parameter at position 0 is not set");
  }

  private List<String> prepareInfo(MariadbConnection connection) {
    return connection
        .createStatement(
            "SHOW SESSION STATUS WHERE Variable_name in ('Prepared_stmt_count','Com_stmt_prepare',"
                + " 'Com_stmt_close')")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1, String.class)))
        .collectList()
        .block();
  }

  @Test
  void cache() throws Throwable {
    Assumptions.assumeTrue(
        isMariaDBServer()
            && !"maxscale".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder
            .clone()
            .useServerPrepStmts(true)
            .prepareCacheSize(3)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(confPipeline).create().block();
    connection
        .createStatement("SELECT ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Long.class)))
        .as(StepVerifier::create)
        .expectNext(1L)
        .verifyComplete();
    connection
        .createStatement("SELECT ?")
        .bind(0, 2)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Long.class)))
        .as(StepVerifier::create)
        .expectNext(2L)
        .verifyComplete();
    connection.close().block();
  }

  @Test
  @SuppressWarnings("unchecked")
  void cacheReuse() throws Throwable {
    Assumptions.assumeTrue(
        isMariaDBServer()
            && !"maxscale".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder
            .clone()
            .useServerPrepStmts(true)
            .prepareCacheSize(3)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(confPipeline).create().block();
    try {
      Method method = connection.getClass().getDeclaredMethod("_test_prepareCache");
      method.setAccessible(true);
      PrepareCache cache = (PrepareCache) method.invoke(connection);
      ServerPrepareResult[] prepareResults = new ServerPrepareResult[5];

      for (long i = 0; i < 5; i++) {

        connection
            .createStatement("SELECT " + i + ", ?")
            .bind(0, i)
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, Long.class)))
            .as(StepVerifier::create)
            .expectNext(i)
            .verifyComplete();

        Object[] entriesArr = cache.entrySet().toArray();
        switch ((int) i) {
          case 0:
            Assertions.assertTrue(entriesArr[0].toString().startsWith("SELECT 0"));
            prepareResults[0] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[0]).getValue();
            break;
          case 1:
            Assertions.assertTrue(entriesArr[0].toString().startsWith("SELECT 0"));
            Assertions.assertTrue(entriesArr[1].toString().startsWith("SELECT 1"));
            prepareResults[1] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[1]).getValue();
            break;
          case 2:
            Assertions.assertTrue(entriesArr[0].toString().startsWith("SELECT 0"));
            Assertions.assertTrue(entriesArr[1].toString().startsWith("SELECT 1"));
            Assertions.assertTrue(entriesArr[2].toString().startsWith("SELECT 2"));
            prepareResults[2] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[2]).getValue();
            break;
          case 3:
            Assertions.assertTrue(entriesArr[0].toString().startsWith("SELECT 2"));
            Assertions.assertTrue(entriesArr[1].toString().startsWith("SELECT 1"));
            Assertions.assertTrue(entriesArr[2].toString().startsWith("SELECT 3"));
            prepareResults[3] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[2]).getValue();
            break;
          case 4:
            Assertions.assertTrue(entriesArr[0].toString().startsWith("SELECT 1"));
            Assertions.assertTrue(entriesArr[1].toString().startsWith("SELECT 3"));
            Assertions.assertTrue(entriesArr[2].toString().startsWith("SELECT 4"));
            prepareResults[4] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[2]).getValue();
            break;
        }

        if (i % 2 == 0) {
          connection
              .createStatement("SELECT 1, ?")
              .bind(0, i)
              .execute()
              .flatMap(r -> r.map((row, metadata) -> row.get(1, Long.class)))
              .as(StepVerifier::create)
              .expectNext(i)
              .verifyComplete();
        }
      }

      Assertions.assertTrue(
          prepareResults[0].toString().contains("closing=true, use=0, cached=false}"));
      Assertions.assertTrue(
          prepareResults[1].toString().contains("closing=false, use=0, cached=true}"));
      Assertions.assertTrue(
          prepareResults[2].toString().contains("closing=true, use=0, cached=false}"));
      Assertions.assertTrue(
          prepareResults[3].toString().contains("closing=false, use=0, cached=true}"));
      Assertions.assertTrue(
          prepareResults[4].toString().contains("closing=false, use=0, cached=true}"));

      List<String> endingStatus = prepareInfo(connection);
      // Com_stmt_prepare
      if (!"maxscale".equals(System.getenv("srv"))
          && !"skysql-ha".equals(System.getenv("srv"))
          && (isMariaDBServer() || !minVersion(8, 0, 0))
          && !isXpand()) {
        Assertions.assertEquals("5", endingStatus.get(1), endingStatus.get(1));
      }

    } finally {
      connection.close().block();
    }
  }
}

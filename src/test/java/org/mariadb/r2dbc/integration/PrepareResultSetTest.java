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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private static List<String> stringList =
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
    sharedConn.createStatement("DROP TABLE IF EXISTS PrepareResultSetTest").execute().blockLast();
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
            "INSERT INTO PrepareResultSetTest VALUES (456,789000002,25,30, 456.45,127,2020,45,'ዩኒኮድ ወረጘ የጝ',65445681355454,987456,45000, 45.9, -2, 2045, 12, 'ዩኒኮድ What does this means ?')")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE PrepareResultSetTest").execute().blockLast();
  }

  @Test
  void parameterLengthEncoded() {
    Assumptions.assumeTrue(maxAllowedPacket() >= 16 * 1024 * 1024);

    char[] arr1024 = new char[1024];
    for (int i = 0; i < arr1024.length; i++) {
      arr1024[i] = (char) ('a' + (i % 10));
    }

    char[] arr = new char[16_000_000];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = (char) ('a' + (i % 10));
    }

    sharedConnPrepare
        .createStatement(
            "CREATE TEMPORARY TABLE parameterLengthEncoded"
                + "(t0 VARCHAR(1024),t1 MEDIUMTEXT) DEFAULT CHARSET=utf8mb4")
        .execute()
        .blockLast();
    sharedConnPrepare
        .createStatement("INSERT INTO parameterLengthEncoded VALUES (?, ?)")
        .bind(0, String.valueOf(arr1024))
        .bind(1, String.valueOf(arr))
        .execute()
        .blockLast();
    sharedConnPrepare
        .createStatement("SELECT * FROM parameterLengthEncoded")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      String t0 = row.get(0, String.class);
                      String t1 = row.get(1, String.class);
                      Assertions.assertEquals(String.valueOf(arr1024), t0);
                      Assertions.assertEquals(String.valueOf(arr), t1);
                      return t0;
                    }))
        .as(StepVerifier::create)
        .expectNext(String.valueOf(arr1024))
        .verifyComplete();
  }

  @Test
  void parameterLengthEncodedLong() {
    Assumptions.assumeTrue(maxAllowedPacket() >= 20 * 1024 * 1024);
    // out of memory on travis and 10.1
    Assumptions.assumeFalse(
        "mariadb:10.1".equals(System.getenv("DB")) || "mysql:5.6".equals(System.getenv("DB")));

    char[] arr = new char[20_000_000];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = (char) ('a' + (i % 10));
    }
    String val = String.valueOf(arr);
    sharedConnPrepare
        .createStatement(
            "CREATE TEMPORARY TABLE parameterLengthEncodedLong"
                + "(t0 LONGTEXT) DEFAULT CHARSET=utf8mb4")
        .execute()
        .blockLast();
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
    sharedConnPrepare
        .createStatement(
            "CREATE TEMPORARY TABLE missingParameter"
                + "(t1 VARCHAR(256),t2 VARCHAR(256)) DEFAULT CHARSET=utf8mb4")
        .execute()
        .blockLast();

    // missing first parameter
    MariadbStatement stmt =
        sharedConnPrepare.createStatement("INSERT INTO missingParameter(t1, t2) VALUES (?, ?)");

    stmt.bind(1, "test").execute().blockLast();

    assertThrows(
        IllegalArgumentException.class, () -> stmt.bind(null, null), "identifier cannot be null");
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bind("test", null),
        "Cannot use getColumn(name) with prepared statement");
    assertThrows(
        IllegalArgumentException.class, () -> stmt.add(), "Parameter at position 0 is not set");
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
  }

  @Test
  void resultSetSkippingRes() {
    for (int i = 10; i < 17; i++) {
      int finalI = i;
      sharedConnPrepare
          .createStatement("SELECT * FROM PrepareResultSetTest WHERE 1 = ?")
          .bind(0, 1)
          .execute()
          .flatMap(
              r ->
                  r.map(
                      (row, metadata) -> {
                        return row.get(finalI, String.class);
                      }))
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
            "CREATE TEMPORARY TABLE INSERT_RETURNING (id int not null primary key auto_increment, test varchar(10))")
        .execute()
        .blockLast();

    sharedConnPrepare
        .createStatement("INSERT INTO INSERT_RETURNING(test) VALUES (?), (?)")
        .bind(0, "test1")
        .bind(1, "test2")
        .fetchSize(44)
        .returnGeneratedValues("id", "test")
        .execute()
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
            "CREATE TEMPORARY TABLE returningBefore105 (id int not null primary key auto_increment, test varchar(10))")
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
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .as(StepVerifier::create)
        .expectNext("3")
        .verifyComplete();

    sharedConnPrepare
        .createStatement("INSERT INTO returningBefore105(test) VALUES (?)")
        .bind(0, "test3")
        .returnGeneratedValues("TEST_COL_NAME")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get("TEST_COL_NAME", String.class)))
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
        "index must be in 0-0 range but value is -1");
    stmt.bind(0, 1).execute().blockLast();
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bind(2, 1),
        "index must be in 0-0 range but value is 2");
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bind(0, this),
        "No encoder for class org.mariadb.r2dbc.integration.PrepareResultSetTest (parameter at index 0) ");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(-1, Integer.class),
        "index must be in 0-0 range but value is -1");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(2, Integer.class),
        "index must be in 0-0 range but value is 2");
    stmt.bindNull(0, this.getClass());
    stmt.execute().blockLast();
    // no parameter
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.execute().blockLast(),
        "Parameter at position 0 is not " + "set");
    assertThrows(
        IllegalArgumentException.class, () -> stmt.add(), "Parameter at position 0 is not set");
  }

  @Test
  void parameterNull() {
    sharedConnPrepare
        .createStatement("CREATE TEMPORARY TABLE parameterNull(t varchar(10), t2 varchar(10))")
        .execute()
        .blockLast();
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

    stmt.bindNull(0, this.getClass())
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
        IllegalArgumentException.class,
        () -> stmt.bindNull("fff", String.class),
        "Cannot use getColumn(name) with prepared statement");
  }

  @Test
  void prepareReuse() {
    MariadbStatement stmt = sharedConnPrepare.createStatement("DO 1 = ?");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bind(-1, 1),
        "wrong index value -1, index must be positive");
    stmt.bind(0, 1).execute().blockLast();
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bind(2, 1),
        "index must be in 0-0 range but value is 2");
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.bind(0, this),
        "No encoder for class org.mariadb.r2dbc.integration.PrepareResultSetTest (parameter at index 0) ");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(-1, Integer.class),
        "wrong index value -1, index must be positive");
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> stmt.bindNull(2, Integer.class),
        "index must be in 0-0 range but value is 2");
    stmt.bindNull(0, this.getClass());
    stmt.execute().blockLast();
    // no parameter
    assertThrows(
        IllegalArgumentException.class,
        () -> stmt.execute().blockLast(),
        "Parameter at position 0 is not set");
  }

  private List<String> prepareInfo(MariadbConnection connection) {
    return connection
        .createStatement(
            "SHOW SESSION STATUS WHERE Variable_name in ('Prepared_stmt_count','Com_stmt_prepare', 'Com_stmt_close')")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1, String.class)))
        .collectList()
        .block();
  }

  @Test
  void cache() throws Throwable {
    MariadbConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder
            .clone()
            .useServerPrepStmts(true)
            .prepareCacheSize(3)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(confPipeline).create().block();
    connection.createStatement("SELECT ?").bind(0, 1).execute().subscribe();
    connection.createStatement("SELECT ?").bind(0, 1).execute().blockLast();
    connection.close().block();
  }

  @Test
  @SuppressWarnings("unchecked")
  void cacheReuse() throws Throwable {
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
          connection.createStatement("SELECT 1, ?").bind(0, i).execute().blockLast();
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
          && (isMariaDBServer() || !minVersion(8, 0, 0))) {
        Assertions.assertEquals("5", endingStatus.get(1), endingStatus.get(1));
      }

    } finally {
      connection.close().block();
    }
  }
}

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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.test.StepVerifier;

public class PrepareResultSetTest extends BaseTest {
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
            Assertions.assertEquals(
                "SELECT 0, ?=ServerPrepareResult{statementId=1, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[0].toString());
            prepareResults[0] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[0]).getValue();
            break;
          case 1:
            Assertions.assertEquals(
                "SELECT 0, ?=ServerPrepareResult{statementId=1, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[0].toString());
            Assertions.assertEquals(
                "SELECT 1, ?=ServerPrepareResult{statementId=2, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[1].toString());
            prepareResults[1] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[1]).getValue();
            break;
          case 2:
            Assertions.assertEquals(
                "SELECT 0, ?=ServerPrepareResult{statementId=1, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[0].toString());
            Assertions.assertEquals(
                "SELECT 1, ?=ServerPrepareResult{statementId=2, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[1].toString());
            Assertions.assertEquals(
                "SELECT 2, ?=ServerPrepareResult{statementId=3, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[2].toString());
            prepareResults[2] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[2]).getValue();
            break;
          case 3:
            Assertions.assertEquals(
                "SELECT 2, ?=ServerPrepareResult{statementId=3, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[0].toString());
            Assertions.assertEquals(
                "SELECT 1, ?=ServerPrepareResult{statementId=2, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[1].toString());
            Assertions.assertEquals(
                "SELECT 3, ?=ServerPrepareResult{statementId=4, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[2].toString());
            prepareResults[3] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[2]).getValue();
            break;
          case 4:
            Assertions.assertEquals(
                "SELECT 1, ?=ServerPrepareResult{statementId=2, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[0].toString());
            Assertions.assertEquals(
                "SELECT 3, ?=ServerPrepareResult{statementId=4, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[1].toString());
            Assertions.assertEquals(
                "SELECT 4, ?=ServerPrepareResult{statementId=5, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
                entriesArr[2].toString());
            prepareResults[4] = ((Map.Entry<String, ServerPrepareResult>) entriesArr[2]).getValue();
            break;
        }

        if (i % 2 == 0) {
          connection.createStatement("SELECT 1, ?").bind(0, i).execute().blockLast();
        }
      }

      Assertions.assertEquals(
          "ServerPrepareResult{statementId=1, numColumns=2, numParams=1, closing=true, use=0, cached=false}",
          prepareResults[0].toString());
      Assertions.assertEquals(
          "ServerPrepareResult{statementId=2, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
          prepareResults[1].toString());
      Assertions.assertEquals(
          "ServerPrepareResult{statementId=3, numColumns=2, numParams=1, closing=true, use=0, cached=false}",
          prepareResults[2].toString());
      Assertions.assertEquals(
          "ServerPrepareResult{statementId=4, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
          prepareResults[3].toString());
      Assertions.assertEquals(
          "ServerPrepareResult{statementId=5, numColumns=2, numParams=1, closing=false, use=0, cached=true}",
          prepareResults[4].toString());

      List<String> endingStatus = prepareInfo(connection);
      // Com_stmt_prepare
      Assertions.assertEquals("5", endingStatus.get(1), endingStatus.get(1));

    } finally {
      connection.close().block();
    }
  }
}

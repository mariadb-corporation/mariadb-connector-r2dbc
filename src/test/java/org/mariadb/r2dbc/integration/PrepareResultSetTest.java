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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
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
            "CREATE TABLE PrepareResultSetTest(id int, i2 BIGINT, i3 int, i4 MEDIUMINT, i5 FLOAT, i6 SMALLINT, i7 YEAR, i8 TINYINT, i9 VARCHAR(256), i10 BIGINT, i11 int, i12 MEDIUMINT, i13 FLOAT, i14 SMALLINT, i15 YEAR, i16 TINYINT, i17 VARCHAR(256))")
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
}

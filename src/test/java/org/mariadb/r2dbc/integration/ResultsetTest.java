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
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class ResultsetTest extends BaseTest {
  private static String vals = "azertyuiopqsdfghjklmwxcvbn";

  @Test
  void multipleResultSet() {
    sharedConn
        .createStatement(
            "create  procedure multiResultSets() BEGIN  SELECT 'a', 'b'; SELECT 'c', 'd', 'e'; END")
        .execute()
        .subscribe();
    final AtomicBoolean first = new AtomicBoolean(true);
    sharedConn
        .createStatement("call multiResultSets()")
        .execute()
        .subscribe(
            res -> {
              if (first.get()) {
                first.set(false);
                res.map(
                        (row, metadata) -> {
                          Assertions.assertEquals(row.get(0), "a");
                          Assertions.assertEquals(row.get(1), "b");
                          return "true";
                        })
                    .subscribe();
              } else {
                res.map(
                        (row, metadata) -> {
                          Assertions.assertEquals(row.get(0), "c");
                          Assertions.assertEquals(row.get(1), "d");
                          Assertions.assertEquals(row.get(2), "e");
                          return "true";
                        })
                    .subscribe();
              }
            });
  }

  private String stLen(int len) {
    StringBuilder sb = new StringBuilder(len);
    Random rand = new Random();
    for (int i = 0; i < len; i++) {
      sb.append(vals.charAt(rand.nextInt(26)));
    }
    return sb.toString();
  }

  @Test
  void readResultSet() {
    String[] first = new String[] {stLen(10), stLen(300), stLen(70000), stLen(1000)};
    String[] second = new String[] {stLen(10), stLen(300), stLen(70000), stLen(1000)};
    String[] third = new String[] {stLen(10), stLen(300), stLen(70000), stLen(1000)};

    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE readResultSet (a TEXT, b TEXT, c LONGTEXT, d TEXT)")
        .execute()
        .subscribe();
    sharedConn
        .createStatement("INSERT INTO readResultSet VALUES (?,?,?,?), (?,?,?,?), (?,?,?,?)")
        .bind(0, first[0])
        .bind(1, first[1])
        .bind(2, first[2])
        .bind(3, first[3])
        .bind(4, second[0])
        .bind(5, second[1])
        .bind(6, second[2])
        .bind(7, second[3])
        .bind(8, third[0])
        .bind(9, third[1])
        .bind(10, third[2])
        .bind(11, third[3])
        .execute()
        .subscribe();

    sharedConn
        .createStatement("SELECT * FROM readResultSet")
        .execute()
        .flatMap(
            res ->
                res.map(
                    (row, metadata) -> {
                      return row.get(3, String.class)
                          + row.get(1, String.class)
                          + row.get(2, String.class)
                          + row.get(0, String.class);
                    }))
        .as(StepVerifier::create)
        .expectNext(
            first[3] + first[1] + first[2] + first[0],
            second[3] + second[1] + second[2] + second[0],
            third[3] + third[1] + third[2] + third[0])
        .verifyComplete();
  }

  @Test
  void getIndexToBig() {
    getIndexToBig(sharedConn);
    getIndexToBig(sharedConnPrepare);
  }

  void getIndexToBig(MariadbConnection connection) {
    connection
        .createStatement("SELECT 1, 2, ?")
        .bind(0, 3)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      return row.get(0, Long.class) + row.get(5, Long.class);
                    }))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("Column index 5 is larger than the number of columns 3"))
        .verify();
  }

  @Test
  void getIndexToLow() {
    getIndexToLow(sharedConn);
    getIndexToLow(sharedConnPrepare);
  }

  void getIndexToLow(MariadbConnection connection) {
    connection
        .createStatement("SELECT 1, 2, ?")
        .bind(0, 3)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      return row.get(0, Long.class) + row.get(-5, Long.class);
                    }))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable.getMessage().equals("Column index -5 must be positive"))
        .verify();
  }
}

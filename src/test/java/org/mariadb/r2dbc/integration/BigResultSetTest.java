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

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import org.mariadb.r2dbc.api.MariadbResult;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class BigResultSetTest extends BaseTest {

  @Test
  void BigResultSet() {
    Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty("RUN_LONG_TEST", "true")));
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    // sequence table requirement
    Assumptions.assumeTrue(meta.isMariaDBServer() && minVersion(10, 1, 0));

    sharedConn
        .createStatement("SELECT * FROM seq_1_to_10000")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNextCount(10000)
        .verifyComplete();
  }

  //  @Test
  //  void bench() {
  //    for (int i = 0; i < 1000000; i++) {
  //      consume(1, sharedConn);
  //    }
  //  }
  //
  //
  //  private void consume(int threadNo, io.r2dbc.spi.Connection connection) {
  //    io.r2dbc.spi.Statement statement =
  //            connection.createStatement("select * FROM benchmarks" + threadNo);
  //    Object[] obj =
  //            Flux.from(statement.execute())
  //                    .flatMap(
  //                            it ->
  //                                    it.map(
  //                                            (row, rowMetadata) -> {
  //                                              Object[] objs = new Object[2];
  //                                              for (int i = 0; i < 2; i++) {
  //                                                objs[i] = row.get(i);
  //                                              }
  //                                              return objs;
  //                                            }))
  //                    .blockLast();
  //  }

  @Test
  void multipleFluxSubscription() {
    Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty("RUN_LONG_TEST", "true")));
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    // sequence table requirement
    Assumptions.assumeTrue(meta.isMariaDBServer() && minVersion(10, 1, 0));
    Flux<MariadbResult> res = sharedConn.createStatement("SELECT * FROM seq_1_to_50000").execute();

    Flux<String> flux1 =
        res.flatMap(r -> r.map((row, metadata) -> row.get(0, String.class))).share();

    AtomicInteger total = new AtomicInteger();

    for (int i = 0; i < 10; i++) {
      flux1.subscribe(
          s -> {
            total.incrementAndGet();
          });
    }

    flux1.blockLast();
    Assertions.assertTrue(total.get() >= 50000);
  }

  @Test
  void multiPacketRow() {
    Assumptions.assumeTrue(
        checkMaxAllowedPacketMore20m(sharedConn)
            && Boolean.parseBoolean(System.getProperty("RUN_LONG_TEST", "true")));
    final char[] array19m = new char[19000000];
    for (int i = 0; i < array19m.length; i++) {
      array19m[i] = (char) (0x30 + (i % 10));
    }

    sharedConn
        .createStatement("CREATE TEMPORARY TABLE multiPacketRow(val LONGTEXT)")
        .execute()
        .subscribe();
    sharedConn
        .createStatement("INSERT INTO multiPacketRow VALUES (?)")
        .bind(0, new String(array19m))
        .execute()
        .blockLast();
    Assertions.assertArrayEquals(
        array19m,
        sharedConn
            .createStatement("SELECT * FROM multiPacketRow")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
            .blockLast()
            .toCharArray());
  }

  public boolean checkMaxAllowedPacketMore20m(MariadbConnection connection) {
    long maxAllowedPacket =
        connection
            .createStatement("select @@max_allowed_packet")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, Long.class)))
            .blockLast();
    return maxAllowedPacket >= 20 * 1024 * 1024L;
  }
}

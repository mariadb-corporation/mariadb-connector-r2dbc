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

package org.mariadb.r2dbc.integration.parameter;

import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class ShortParameterTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE SmallIntParam (t1 SMALLINT, t2 SMALLINT, t3 SMALLINT)")
        .execute()
        .subscribe();
    // ensure having same kind of result for truncation
    sharedConn
        .createStatement("SET @@sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
        .execute()
        .blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE SmallIntParam").execute().blockLast();
  }

  @Test
  void nullValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bindNull(0, BigInteger.class)
        .bindNull(1, BigInteger.class)
        .bindNull(2, BigInteger.class)
        .execute()
        .blockLast();
    validate(Optional.empty(), Optional.empty(), Optional.empty());
  }

  @Test
  void bigIntValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, BigInteger.ONE)
        .bind(1, new BigInteger("32767"))
        .bind(2, new BigInteger("-9"))
        .execute()
        .blockLast();
    validate(
        Optional.of(Short.valueOf("1")),
        Optional.of(Short.valueOf("32767")),
        Optional.of(Short.valueOf("-9")));
  }

  @Test
  void stringValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, "1")
        .bind(1, "32767")
        .bind(2, "-9")
        .execute()
        .blockLast();
    validate(
        Optional.of(Short.valueOf("1")),
        Optional.of(Short.valueOf("32767")),
        Optional.of(Short.valueOf("-9")));
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, BigDecimal.ONE)
        .bind(1, new BigDecimal("32767"))
        .bind(2, new BigDecimal("-9"))
        .execute()
        .blockLast();
    validate(
        Optional.of(Short.valueOf("1")),
        Optional.of(Short.valueOf("32767")),
        Optional.of(Short.valueOf("-9")));
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, 1)
        .bind(1, 32767)
        .bind(2, -9)
        .execute()
        .blockLast();
    validate(
        Optional.of(Short.valueOf("1")),
        Optional.of(Short.valueOf("32767")),
        Optional.of(Short.valueOf("-9")));
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, (byte) 127)
        .bind(1, (byte) -128)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validate(
        Optional.of(Short.valueOf("127")),
        Optional.of(Short.valueOf("-128")),
        Optional.of(Short.valueOf("0")));
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, 127f)
        .bind(1, -128f)
        .bind(2, 0f)
        .execute()
        .blockLast();
    validate(
        Optional.of(Short.valueOf("127")),
        Optional.of(Short.valueOf("-128")),
        Optional.of(Short.valueOf("0")));
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, 127d)
        .bind(1, -128d)
        .bind(2, 0d)
        .execute()
        .blockLast();
    validate(
        Optional.of(Short.valueOf("127")),
        Optional.of(Short.valueOf("-128")),
        Optional.of(Short.valueOf("0")));
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("1"))
        .bind(1, Short.valueOf("-1"))
        .bind(2, Short.valueOf("0"))
        .execute()
        .blockLast();
    validate(
        Optional.of(Short.valueOf("1")),
        Optional.of(Short.valueOf("-1")),
        Optional.of(Short.valueOf("0")));
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, Long.valueOf("1"))
        .bind(1, Long.valueOf("-1"))
        .bind(2, Long.valueOf("0"))
        .execute()
        .blockLast();
    validate(
        Optional.of(Short.valueOf("1")),
        Optional.of(Short.valueOf("-1")),
        Optional.of(Short.valueOf("0")));
  }

  @Test
  void localDateTimeValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.now())
        .bind(1, LocalDateTime.now())
        .bind(2, LocalDateTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
  }

  @Test
  void localDateValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, LocalDate.now())
        .bind(1, LocalDate.now())
        .bind(2, LocalDate.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
  }

  @Test
  void localTimeValue() {
    sharedConn
        .createStatement("INSERT INTO SmallIntParam VALUES (?,?,?)")
        .bind(0, LocalTime.now())
        .bind(1, LocalTime.now())
        .bind(2, LocalTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
  }

  private void validate(Optional<Short> t1, Optional<Short> t2, Optional<Short> t3) {
    sharedConn
        .createStatement("SELECT * FROM SmallIntParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            Optional.ofNullable((Short) row.get(0)),
                            Optional.ofNullable(row.get(1)),
                            Optional.ofNullable(row.get(2)))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}

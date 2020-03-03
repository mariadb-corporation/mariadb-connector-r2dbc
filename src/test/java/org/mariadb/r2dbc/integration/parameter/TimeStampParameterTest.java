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

import io.r2dbc.spi.R2dbcBadGrammarException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

public class TimeStampParameterTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement(
            "CREATE TEMPORARY TABLE TimestampParam (t1 TIMESTAMP(6) NULL, t2 TIMESTAMP(6) NULL, t3 TIMESTAMP(6) NULL)")
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
    sharedConn.createStatement("TRUNCATE TABLE TimestampParam").execute().blockLast();
  }

  @Test
  void bigIntValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, BigInteger.ONE)
        .bind(1, new BigInteger("9223372036854775807"))
        .bind(2, new BigInteger("-9"))
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void stringValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, "1")
        .bind(1, "9223372036854775807")
        .bind(2, "-9")
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, BigDecimal.ONE)
        .bind(1, new BigDecimal("9223372036854775807"))
        .bind(2, new BigDecimal("-9"))
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, 1)
        .bind(1, -1)
        .bind(2, 0)
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, (byte) 127)
        .bind(1, (byte) 128)
        .bind(2, (byte) 0)
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, 127f)
        .bind(1, -128f)
        .bind(2, 0f)
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, 127d)
        .bind(1, -128d)
        .bind(2, 0d)
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("1"))
        .bind(1, Short.valueOf("-1"))
        .bind(2, Short.valueOf("0"))
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, Long.valueOf("1"))
        .bind(1, Long.valueOf("-1"))
        .bind(2, Long.valueOf("0"))
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void localDateTimeValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.parse("2010-01-12T05:08:09.0014"))
        .bind(1, LocalDateTime.parse("2018-12-15T05:08:10.123456"))
        .bind(2, LocalDateTime.parse("2025-05-12T05:08:11.123"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalDateTime.parse("2010-01-12T05:08:09.001400")),
        Optional.of(LocalDateTime.parse("2018-12-15T05:08:10.123456")),
        Optional.of(LocalDateTime.parse("2025-05-12T05:08:11.123000")));
  }

  @Test
  void localDateValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, LocalDate.parse("2010-01-12"))
        .bind(1, LocalDate.parse("2018-12-15"))
        .bind(2, LocalDate.parse("2025-05-12"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalDateTime.parse("2010-01-12T00:00:00.000000")),
        Optional.of(LocalDateTime.parse("2018-12-15T00:00:00.000000")),
        Optional.of(LocalDateTime.parse("2025-05-12T00:00:00.000000")));
  }

  @Test
  void localTimeValue() {
    sharedConn
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, LocalTime.now())
        .bind(1, LocalTime.now())
        .bind(2, LocalTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  private void validate(
      Optional<LocalDateTime> t1, Optional<LocalDateTime> t2, Optional<LocalDateTime> t3) {
    sharedConn
        .createStatement("SELECT * FROM TimestampParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            Optional.ofNullable((LocalDateTime) row.get(0)),
                            Optional.ofNullable(row.get(1)),
                            Optional.ofNullable(row.get(2)))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}

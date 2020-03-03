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

package org.mariadb.r2dbc.integration.codec;

import io.r2dbc.spi.R2dbcTransientResourceException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

public class DateTimeParseTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE DateTimeTable (t1 DATETIME(6) NULL)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO DateTimeTable VALUES('2013-07-22 12:50:05.01230'), ('2035-01-31 10:45:01'), (null)")
        .execute()
        .blockLast();
    // ensure having same kind of result for truncation
    sharedConn
        .createStatement("SET @@sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
        .execute()
        .blockLast();
  }

  @Test
  void defaultValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalDateTime.parse("2013-07-22T12:50:05.01230")),
            Optional.of(LocalDateTime.parse("2035-01-31T10:45:01")),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void localDateValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDate.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalDate.parse("2013-07-22")),
            Optional.of(LocalDate.parse("2035-01-31")),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void localTimeValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalTime.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalTime.parse("12:50:05.012300")),
            Optional.of(LocalTime.parse("10:45:01")),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void booleanValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Boolean and column type DATETIME"))
        .verify();
  }

  @Test
  void byteArrayValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, byte[].class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type DATETIME"))
        .verify();
  }

  @Test
  void ByteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Byte and column type DATETIME"))
        .verify();
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte and column type DATETIME"))
        .verify();
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Short and column type DATETIME"))
        .verify();
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Integer and column type DATETIME"))
        .verify();
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Long and column type DATETIME"))
        .verify();
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Float and column type DATETIME"))
        .verify();
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Double and column type DATETIME"))
        .verify();
  }

  @Test
  void stringValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of("2013-07-22 12:50:05.012300"),
            Optional.of("2035-01-31 10:45:01.000000"),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "No decoder for type java.math.BigDecimal and column type DATETIME"))
        .verify();
  }

  @Test
  void bigintValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "No decoder for type java.math.BigInteger and column type DATETIME"))
        .verify();
  }

  @Test
  void localDateTimeValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DateTimeTable")
        .execute()
        .flatMap(
            r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDateTime.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalDateTime.parse("2013-07-22T12:50:05.01230")),
            Optional.of(LocalDateTime.parse("2035-01-31T10:45:01")),
            Optional.empty())
        .verifyComplete();
  }
}

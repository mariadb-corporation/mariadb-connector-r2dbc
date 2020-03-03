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
import java.util.Optional;

public class MediumIntParseTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE MediumIntTable " + "(t1 MEDIUMINT)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("INSERT INTO MediumIntTable VALUES (0),(1),(-1), (null)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE MediumIntUnsignedTable (t1 MEDIUMINT UNSIGNED)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("INSERT INTO MediumIntUnsignedTable VALUES (0), (1), (16777215), (null)")
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
        .createStatement("SELECT t1 FROM MediumIntTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1), Optional.of(-1), Optional.empty())
        .verifyComplete();
  }

  @Test
  void defaultUnsignedValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1), Optional.of(16777215), Optional.empty())
        .verifyComplete();
  }

  @Test
  void booleanValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(false), Optional.of(true), Optional.of(false), Optional.empty())
        .verifyComplete();
  }

  @Test
  void byteArrayValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, byte[].class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type MEDIUMINT"))
        .verify();
  }

  @Test
  void ByteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Byte and column type MEDIUMINT"))
        .verify();
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte and column type MEDIUMINT"))
        .verify();
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Short and column type MEDIUMINT"))
        .verify();
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1), Optional.of(-1), Optional.empty())
        .verifyComplete();
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0L), Optional.of(1L), Optional.of(-1L), Optional.empty())
        .verifyComplete();
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0F), Optional.of(1F), Optional.of(-1F), Optional.empty())
        .verifyComplete();
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0D), Optional.of(1D), Optional.of(-1D), Optional.empty())
        .verifyComplete();
  }

  @Test
  void stringValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("0"), Optional.of("1"), Optional.of("-1"), Optional.empty())
        .verifyComplete();
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigDecimal.ZERO),
            Optional.of(BigDecimal.ONE),
            Optional.of(BigDecimal.valueOf(-1)),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void bigintValue() {
    sharedConn
        .createStatement("SELECT t1 FROM MediumIntTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigInteger.ZERO),
            Optional.of(BigInteger.ONE),
            Optional.of(BigInteger.valueOf(-1)),
            Optional.empty())
        .verifyComplete();
  }
}

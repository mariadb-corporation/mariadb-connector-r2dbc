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

public class DecimalParseTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE DecimalTable (t1 DECIMAL(40,20))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO DecimalTable VALUES (0.1),(1),(9223372036854775807.9223372036854775807), (null)")
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
        .createStatement("SELECT t1 FROM DecimalTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigDecimal("0.10000000000000000000")),
            Optional.of(new BigDecimal("1.00000000000000000000")),
            Optional.of(new BigDecimal("9223372036854775807.92233720368547758070")),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void booleanValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Boolean and column type DECIMAL"))
        .verify();
  }

  @Test
  void byteArrayValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, byte[].class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type DECIMAL"))
        .verify();
  }

  @Test
  void ByteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Byte and column type DECIMAL"))
        .verify();
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte and column type DECIMAL"))
        .verify();
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Short and column type DECIMAL"))
        .verify();
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "Out of range value for column 't1' : value 9223372036854775807  is not in java.lang.Integer range"))
        .verify();
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Long and column type DECIMAL"))
        .verify();
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(0.1F),
            Optional.of(1F),
            Optional.of(9223372036854775807.9223372036854775807F),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(0.1D),
            Optional.of(1D),
            Optional.of(9223372036854775807.9223372036854775807D),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void stringValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of("0.10000000000000000000"),
            Optional.of("1.00000000000000000000"),
            Optional.of("9223372036854775807.92233720368547758070"),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigDecimal("0.10000000000000000000")),
            Optional.of(new BigDecimal("1.00000000000000000000")),
            Optional.of(new BigDecimal("9223372036854775807.92233720368547758070")),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void bigintValue() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigInteger.ZERO),
            Optional.of(BigInteger.ONE),
            Optional.of(new BigInteger("9223372036854775807")),
            Optional.empty())
        .verifyComplete();
  }
}

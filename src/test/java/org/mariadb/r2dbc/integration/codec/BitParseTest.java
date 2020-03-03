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
import java.util.BitSet;
import java.util.Optional;

public class BitParseTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn.createStatement("CREATE TEMPORARY TABLE BitTable (t1 BIT(4))").execute().blockLast();
    sharedConn
        .createStatement("INSERT INTO BitTable VALUES (b'0001'),(b'1111'),(b'1010'), (null)")
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
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BitSet.valueOf(new byte[] {(byte) 1})),
            Optional.of(BitSet.valueOf(new byte[] {(byte) 15})),
            Optional.of(BitSet.valueOf(new byte[] {(byte) 10})),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void booleanValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(true), Optional.of(true), Optional.of(false), Optional.empty())
        .verifyComplete();
  }

  @Test
  void byteArrayValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte[].class))))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              if (val.get().length != 1) return false;
              return val.get()[0] == (byte) 1;
            })
        .expectNextMatches(
            val -> {
              if (val.get().length != 1) return false;
              return val.get()[0] == (byte) 15;
            })
        .expectNextMatches(
            val -> {
              if (val.get().length != 1) return false;
              return val.get()[0] == (byte) 10;
            })
        .expectNextMatches(val -> !val.isPresent())
        .verifyComplete();
  }

  @Test
  void ByteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((byte) 1), Optional.of((byte) 15), Optional.of((byte) 10), Optional.empty())
        .verifyComplete();
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable LIMIT 3")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((byte) 1), Optional.of((byte) 15), Optional.of((byte) 10))
        .verifyComplete();
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) 1),
            Optional.of((short) 15),
            Optional.of((short) 10),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(1), Optional.of(15), Optional.of(10), Optional.empty())
        .verifyComplete();
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(1L), Optional.of(15L), Optional.of(10L), Optional.empty())
        .verifyComplete();
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Float and column type BIT"))
        .verify();
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Double and column type BIT"))
        .verify();
  }

  @Test
  void stringValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of("b'1'"), Optional.of("b'1111'"), Optional.of("b'1010'"), Optional.empty())
        .verifyComplete();
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigDecimal("1")),
            Optional.of(new BigDecimal("15")),
            Optional.of(new BigDecimal("10")),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void bigintValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigInteger.ONE),
            Optional.of(BigInteger.valueOf(15)),
            Optional.of(BigInteger.valueOf(10)),
            Optional.empty())
        .verifyComplete();
  }
}

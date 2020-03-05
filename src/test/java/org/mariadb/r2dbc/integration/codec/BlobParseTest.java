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

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import reactor.test.StepVerifier;

public class BlobParseTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE BlobTable (t1 BLOB, t2 int)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO BlobTable VALUES ('diego🤘💪',1),('georg',2),('lawrin',3), (null,4)")
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
    String[] expectedVals = new String[] {"diego🤘💪", "georg", "lawrin"};
    AtomicInteger index = new AtomicInteger();

    Consumer<? super ByteBuffer> consumer =
        actual -> {
          byte[] expected = expectedVals[index.getAndIncrement()].getBytes(StandardCharsets.UTF_8);
          if (actual.hasArray()) {
            Assertions.assertArrayEquals(actual.array(), expected);
          } else {
            byte[] res = new byte[actual.remaining()];
            actual.get(res);
            Assertions.assertArrayEquals(res, expected);
          }
        };

    sharedConn
        .createStatement("SELECT t1,t2 FROM BlobTable limit 3")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      row.get(0);
                      row.get(0);
                      row.get(1);
                      row.get(1);
                      return row.get(0);
                    }))
        .cast(Blob.class)
        .flatMap(Blob::stream)
        .as(StepVerifier::create)
        .consumeNextWith(consumer)
        .consumeNextWith(consumer)
        .consumeNextWith(consumer)
        .verifyComplete();
  }

  @Test
  void booleanValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Boolean and column type BLOB"))
        .verify();
  }

  @Test
  void byteArrayValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte[].class))))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> Arrays.equals(val.get(), "diego🤘💪".getBytes(StandardCharsets.UTF_8)))
        .expectNextMatches(
            val -> Arrays.equals(val.get(), "georg".getBytes(StandardCharsets.UTF_8)))
        .expectNextMatches(
            val -> Arrays.equals(val.get(), "lawrin".getBytes(StandardCharsets.UTF_8)))
        .expectNextMatches(val -> !val.isPresent())
        .verifyComplete();
  }

  @Test
  void ByteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Byte and column type BLOB"))
        .verify();
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte and column type BLOB"))
        .verify();
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Short and column type BLOB"))
        .verify();
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Integer and column type BLOB"))
        .verify();
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Long and column type BLOB"))
        .verify();
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Float and column type BLOB"))
        .verify();
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Double and column type BLOB"))
        .verify();
  }

  @Test
  void stringValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.String and column type BLOB"))
        .verify();
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.math.BigDecimal and column type BLOB"))
        .verify();
  }

  @Test
  void bigintValue() {
    sharedConn
        .createStatement("SELECT t1 FROM BlobTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.math.BigInteger and column type BLOB"))
        .verify();
  }
}

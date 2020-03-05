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

import io.r2dbc.spi.Clob;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class StringParseTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE StringTable (t1 varchar(256))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("INSERT INTO StringTable VALUES ('some'),('1'),('0'), (null)")
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
        .createStatement("SELECT t1 FROM StringTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("some"), Optional.of("1"), Optional.of("0"), Optional.empty())
        .verifyComplete();
  }

  @Test
  void clobValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable limit 2")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Mono.from(row.get(0, Clob.class).stream())))
        .as(StepVerifier::create)
        .assertNext(
            val ->
                val.handle(
                        (ch, sink) -> {
                          if (ch.equals("some")) {
                            sink.complete();
                          } else {
                            sink.error(new Exception("ERROR"));
                          }
                        })
                    .subscribe())
        .assertNext(
            val ->
                val.handle(
                        (ch, sink) -> {
                          if (ch.equals("1")) {
                            sink.complete();
                          } else {
                            sink.error(new Exception("ERROR"));
                          }
                        })
                    .subscribe())
        .verifyComplete();
  }

  @Test
  void booleanValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(false), Optional.of(true), Optional.of(false), Optional.empty())
        .verifyComplete();
  }

  @Test
  void byteArrayValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte[].class))))
        .as(StepVerifier::create)
        .expectNextMatches(val -> Arrays.equals(val.get(), "some".getBytes(StandardCharsets.UTF_8)))
        .expectNextMatches(val -> Arrays.equals(val.get(), "1".getBytes(StandardCharsets.UTF_8)))
        .expectNextMatches(val -> Arrays.equals(val.get(), "0".getBytes(StandardCharsets.UTF_8)))
        .expectNextMatches(val -> !val.isPresent())
        .verifyComplete();
  }

  @Test
  void ByteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Byte and column type VARSTRING"))
        .verify();
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte and column type VARSTRING"))
        .verify();
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Short and column type VARSTRING"))
        .verify();
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Integer and column type VARSTRING"))
        .verify();
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Long and column type VARSTRING"))
        .verify();
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Float and column type VARSTRING"))
        .verify();
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Double and column type VARSTRING"))
        .verify();
  }

  @Test
  void stringValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("some"), Optional.of("1"), Optional.of("0"), Optional.empty())
        .verifyComplete();
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "No decoder for type java.math.BigDecimal and column type VARSTRING"))
        .verify();
  }

  @Test
  void bigintValue() {
    sharedConn
        .createStatement("SELECT t1 FROM StringTable LIMIT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "No decoder for type java.math.BigInteger and column type VARSTRING"))
        .verify();
  }
}

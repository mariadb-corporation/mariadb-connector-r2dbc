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

import io.r2dbc.spi.Blob;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

public class BlobParameterTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE BlobParam (t1 BLOB, t2 BLOB, t3 BLOB)")
        .execute()
        .blockLast();
    // ensure having same kind of result for truncation
    sharedConn
        .createStatement("SET @@sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
        .execute()
        .blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE BlobParam").execute().blockLast();
  }

  @Test
  void nullValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bindNull(0, byte[].class)
        .bindNull(1, byte[].class)
        .bindNull(2, byte[].class)
        .execute()
        .blockLast();

    validate(Optional.empty(), Optional.empty(), Optional.empty());
  }

  @Test
  void bigIntValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, new BigInteger("11"))
        .bind(1, new BigInteger("512"))
        .bind(2, new BigInteger("1"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  @Test
  void stringValue() {
    ByteBuffer bb = ByteBuffer.wrap("ô\0你好".getBytes());
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, "\1")
        .bind(1, "A")
        .bind(2, "ô\0你好")
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("\1".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("A".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("ô\0你好".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, new BigDecimal("11"))
        .bind(1, new BigDecimal("512"))
        .bind(2, new BigDecimal("1"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11)
        .bind(1, 512)
        .bind(2, 1)
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, (byte) 15)
        .bind(1, (byte) 127)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("15".getBytes()),
        ByteBuffer.wrap("127".getBytes()),
        ByteBuffer.wrap("0".getBytes()));
  }

  @Test
  void blobValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 15}))))
        .bind(1, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 1, 0, (byte) 127}))))
        .bind(2, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {0}))))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap(new byte[] {(byte) 15}),
        ByteBuffer.wrap(new byte[] {(byte) 1, 0, (byte) 127}),
        ByteBuffer.wrap(new byte[] {0}));
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11f)
        .bind(1, 512f)
        .bind(2, 1f)
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11.0".getBytes()),
        ByteBuffer.wrap("512.0".getBytes()),
        ByteBuffer.wrap("1.0".getBytes()));
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11d)
        .bind(1, 512d)
        .bind(2, 1d)
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11.0".getBytes()),
        ByteBuffer.wrap("512.0".getBytes()),
        ByteBuffer.wrap("1.0".getBytes()));
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("11"))
        .bind(1, Short.valueOf("127"))
        .bind(2, Short.valueOf("1"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("127".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11L)
        .bind(1, 512L)
        .bind(2, 1L)
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  @Test
  void localDateTimeValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.parse("2013-07-22T12:50:05.01230"))
        .bind(1, LocalDateTime.parse("2035-01-31T10:45:01"))
        .bind(2, LocalDateTime.parse("2025-01-31T10:45:01.123"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("2013-07-22 12:50:05.012300".getBytes()),
        ByteBuffer.wrap("2035-01-31 10:45:01".getBytes()),
        ByteBuffer.wrap("2025-01-31 10:45:01.123000".getBytes()));
  }

  @Test
  void localDateValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, LocalDate.parse("2010-01-12"))
        .bind(1, LocalDate.parse("2019-01-31"))
        .bind(2, LocalDate.parse("2019-12-31"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("2010-01-12".getBytes()),
        ByteBuffer.wrap("2019-01-31".getBytes()),
        ByteBuffer.wrap("2019-12-31".getBytes()));
  }

  @Test
  void localTimeValue() {
    sharedConn
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, LocalTime.parse("18:00:00.012340"))
        .bind(1, LocalTime.parse("08:00:00.123"))
        .bind(2, LocalTime.parse("08:00:00.123"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("18:00:00.012340".getBytes()),
        ByteBuffer.wrap("08:00:00.123".getBytes()),
        ByteBuffer.wrap("08:00:00.123".getBytes()));
  }

  private void validate(Optional<ByteBuffer> t1, Optional<ByteBuffer> t2, Optional<ByteBuffer> t3) {
    sharedConn
        .createStatement("SELECT * FROM BlobParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Blob obj0 = (Blob) row.get(0);
                      Blob obj1 = (Blob) row.get(1);
                      Blob obj2 = (Blob) row.get(2);
                      return Flux.just(
                          (obj0 == null)
                              ? Optional.empty()
                              : Optional.of(Mono.from((obj0).stream()).block()),
                          (obj1 == null)
                              ? Optional.empty()
                              : Optional.of(Mono.from((obj1).stream()).block()),
                          (obj2 == null)
                              ? Optional.empty()
                              : Optional.of(Mono.from((obj2).stream()).block()));
                    }))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }

  private void validateNotNull(ByteBuffer t1, ByteBuffer t2, ByteBuffer t3) {
    validateNotNull(t1, 0);
    validateNotNull(t2, 1);
    validateNotNull(t3, 2);
  }

  private void validateNotNull(ByteBuffer t1, int index) {
    sharedConn
        .createStatement("SELECT * FROM BlobParam")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(index, Blob.class)))
        .flatMap(Blob::stream)
        .as(StepVerifier::create)
        .consumeNextWith(
            actual -> {
              if (actual.hasArray() && t1.hasArray()) {
                Assertions.assertArrayEquals(actual.array(), t1.array());
              } else {
                byte[] res = new byte[actual.remaining()];
                actual.get(res);

                byte[] exp = new byte[t1.remaining()];
                t1.get(exp);
                Assertions.assertArrayEquals(res, exp);
              }
            })
        .verifyComplete();
  }
}

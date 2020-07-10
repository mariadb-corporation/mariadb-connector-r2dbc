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
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class BlobParseTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn.createStatement("CREATE TABLE BlobTable (t1 BLOB, t2 int)").execute().blockLast();
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

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE BlobTable").execute().blockLast();
  }

  @Test
  void defaultValue() {
    defaultValue(sharedConn);
  }

  @Test
  void defaultValuePrepare() {
    defaultValue(sharedConnPrepare);
  }

  private void defaultValue(MariadbConnection connection) {

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

    connection
        .createStatement("SELECT t1,t2 FROM BlobTable WHERE 1 = ? limit 3")
        .bind(0, 1)
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
    booleanValue(sharedConn);
  }

  @Test
  void booleanValuePrepare() {
    booleanValue(sharedConnPrepare);
  }

  private void booleanValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
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
    byteArrayValue(sharedConn);
  }

  @Test
  void byteArrayValuePrepare() {
    byteArrayValue(sharedConnPrepare);
  }

  private void byteArrayValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ?")
        .bind(0, 1)
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
    ByteValue(sharedConn);
  }

  @Test
  void ByteValuePrepare() {
    ByteValue(sharedConnPrepare);
  }

  private void ByteValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((byte) 100),
            Optional.of((byte) 103),
            Optional.of((byte) 108),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void byteValue() {
    byteValue(sharedConn);
  }

  @Test
  void byteValuePrepare() {
    byteValue(sharedConnPrepare);
  }

  private void byteValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((byte) 100), Optional.of((byte) 103), Optional.of((byte) 108))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable.getMessage().equals("Cannot return null for primitive byte"))
        .verify();
  }

  @Test
  void shortValue() {
    shortValue(sharedConn);
  }

  @Test
  void shortValuePrepare() {
    shortValue(sharedConnPrepare);
  }

  private void shortValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
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
    intValue(sharedConn);
  }

  @Test
  void intValuePrepare() {
    intValue(sharedConnPrepare);
  }

  private void intValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
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
    longValue(sharedConn);
  }

  @Test
  void longValuePrepare() {
    longValue(sharedConnPrepare);
  }

  private void longValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
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
    floatValue(sharedConn);
  }

  @Test
  void floatValuePrepare() {
    floatValue(sharedConnPrepare);
  }

  private void floatValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
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
    doubleValue(sharedConn);
  }

  @Test
  void doubleValuePrepare() {
    doubleValue(sharedConnPrepare);
  }

  private void doubleValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
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
    stringValue(sharedConn);
  }

  @Test
  void stringValuePrepare() {
    stringValue(sharedConnPrepare);
  }

  private void stringValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
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
  void blobValue() {
    blobValue(sharedConn);
  }

  @Test
  void blobValuePrepare() {
    blobValue(sharedConnPrepare);
  }

  private void blobValue(MariadbConnection connection) {

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

    connection
        .createStatement("SELECT t1,t2 FROM BlobTable WHERE 1 = ? limit 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Blob.class)))
        .cast(Blob.class)
        .flatMap(Blob::stream)
        .as(StepVerifier::create)
        .consumeNextWith(consumer)
        .consumeNextWith(consumer)
        .consumeNextWith(consumer)
        .verifyComplete();
  }

  @Test
  void streamValue() {
    streamValue(sharedConn);
  }

  @Test
  void streamValuePrepare() {
    streamValue(sharedConnPrepare);
  }

  String[] expectedVals = new String[] {"diego🤘💪", "georg", "lawrin"};
  AtomicInteger index = new AtomicInteger();

  private boolean inputStreamToByte(InputStream actual) {
    byte[] expected = expectedVals[index.getAndIncrement()].getBytes(StandardCharsets.UTF_8);
    byte[] val = new byte[expected.length];
    byte[] array = new byte[4096];
    int len;
    int pos = 0;
    try {
      while ((len = actual.read(array)) > 0) {
        System.arraycopy(array, 0, val, pos, len);
        pos += len;
      }
    } catch (IOException ioe) {
      return false;
    }
    if (pos != expected.length) return false;
    Assertions.assertArrayEquals(val, expected);
    return true;
  }

  private void streamValue(MariadbConnection connection) {
    index.set(0);
    connection
        .createStatement("SELECT t1,t2 FROM BlobTable WHERE 1 = ? limit 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, InputStream.class)))
        .as(StepVerifier::create)
        .expectNextMatches(val -> inputStreamToByte(val))
        .expectNextMatches(val -> inputStreamToByte(val))
        .expectNextMatches(val -> inputStreamToByte(val))
        .verifyComplete();
  }

  @Test
  void decimalValue() {
    decimalValue(sharedConn);
  }

  @Test
  void decimalValuePrepare() {
    decimalValue(sharedConnPrepare);
  }

  private void decimalValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
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
    bigintValue(sharedConn);
  }

  @Test
  void bigintValuePrepare() {
    bigintValue(sharedConnPrepare);
  }

  private void bigintValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BlobTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
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

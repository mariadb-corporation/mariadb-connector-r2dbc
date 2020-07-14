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

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class MediumIntParseTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TABLE MediumIntTable (t1 MEDIUMINT, t2 MEDIUMINT ZEROFILL)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("INSERT INTO MediumIntTable VALUES (0, 0),(1, 10),(-1, 100), (null, null)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("CREATE TABLE MediumIntUnsignedTable (t1 MEDIUMINT UNSIGNED)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("INSERT INTO MediumIntUnsignedTable VALUES (0), (1), (16777215), (null)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE MediumIntTable").execute().blockLast();
    sharedConn.createStatement("DROP TABLE MediumIntUnsignedTable").execute().blockLast();
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
    connection
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1), Optional.of(-1), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1), Optional.of(16777215), Optional.empty())
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(false), Optional.of(true), Optional.of(true), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(false), Optional.of(true), Optional.of(true), Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, byte[].class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type MEDIUMINT(signed)"))
        .verify();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, byte[].class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type MEDIUMINT(unsigned)"))
        .verify();
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((byte) 0), Optional.of((byte) 1), Optional.of((byte) -1), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((byte) 0), Optional.of((byte) 1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().equals("byte overflow"))
        .verify();
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ? LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((byte) 0), Optional.of((byte) 1), Optional.of((byte) -1))
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ? LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((byte) 0), Optional.of((byte) 1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().equals("byte overflow"))
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) 0),
            Optional.of((short) 1),
            Optional.of((short) -1),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((short) 0), Optional.of((short) 1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().equals("Short overflow"))
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1), Optional.of(-1), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1), Optional.of(16777215), Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0L), Optional.of(1L), Optional.of(-1L), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0L), Optional.of(1L), Optional.of(16777215L), Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0F), Optional.of(1F), Optional.of(-1F), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0F), Optional.of(1F), Optional.of(16777215F), Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0D), Optional.of(1D), Optional.of(-1D), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0D), Optional.of(1D), Optional.of(16777215D), Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("0"), Optional.of("1"), Optional.of("-1"), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of("00000000"),
            Optional.of("00000010"),
            Optional.of("00000100"),
            Optional.empty())
        .verifyComplete();

    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("0"), Optional.of("1"), Optional.of("16777215"), Optional.empty())
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigDecimal.ZERO),
            Optional.of(BigDecimal.ONE),
            Optional.of(BigDecimal.valueOf(-1)),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigDecimal.ZERO),
            Optional.of(BigDecimal.ONE),
            Optional.of(BigDecimal.valueOf(16777215)),
            Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigInteger.ZERO),
            Optional.of(BigInteger.ONE),
            Optional.of(BigInteger.valueOf(-1)),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigInteger.ZERO),
            Optional.of(BigInteger.ONE),
            Optional.of(BigInteger.valueOf(16777215)),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void meta() {
    meta(sharedConn);
  }

  @Test
  void metaPrepare() {
    meta(sharedConnPrepare);
  }

  private void meta(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM MediumIntTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getJavaType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(Integer.class))
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM MediumIntUnsignedTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getJavaType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(Integer.class))
        .verifyComplete();
  }
}

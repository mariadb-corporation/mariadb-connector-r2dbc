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
import java.time.LocalDate;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.test.StepVerifier;

public class YearParseTest extends BaseTest {
  private static MariadbConnectionMetadata meta = sharedConn.getMetadata();

  @BeforeAll
  public static void before2() {
    String sqlCreate = "CREATE TABLE YearTable (t1 YEAR(4), t2 YEAR(2))";
    String sqlInsert = "INSERT INTO YearTable VALUES (2060, 60),(2071, 71),(0, 0), (null, null)";
    // mysql doesn't support YEAR(2) anymore
    if (!meta.isMariaDBServer()) {
      sqlCreate = "CREATE TABLE YearTable (t1 YEAR(4), t2 YEAR(4))";
      //      sqlInsert = "INSERT INTO YearTable VALUES (2060, 2060),(2071, 1971),(0, 2000), (null,
      // null)";
    }

    sharedConn.createStatement(sqlCreate).execute().blockLast();
    sharedConn.createStatement(sqlInsert).execute().blockLast();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE YearTable").execute().blockLast();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) 2060),
            Optional.of((short) 2071),
            Optional.of((short) 0),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) (meta.isMariaDBServer() ? 60 : 2060)),
            Optional.of((short) (meta.isMariaDBServer() ? 71 : 1971)),
            Optional.of((short) 0),
            Optional.empty())
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
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Boolean and column type YEAR"))
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, byte[].class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type YEAR"))
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) 2060),
            Optional.of((short) 2071),
            Optional.of((short) 0),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) (meta.isMariaDBServer() ? 60 : 2060)),
            Optional.of((short) (meta.isMariaDBServer() ? 71 : 1971)),
            Optional.of((short) (0)),
            Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(2060), Optional.of(2071), Optional.of(0), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? 60 : 2060),
            Optional.of(meta.isMariaDBServer() ? 71 : 1971),
            Optional.of(0),
            Optional.empty())
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(2060L), Optional.of(2071L), Optional.of(0L), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? 60L : 2060L),
            Optional.of(meta.isMariaDBServer() ? 71L : 1971L),
            Optional.of(0L),
            Optional.empty())
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(2060f), Optional.of(2071f), Optional.of(0f), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? 60F : 2060F),
            Optional.of(meta.isMariaDBServer() ? 71F : 1971F),
            Optional.of(0F),
            Optional.empty())
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(2060D), Optional.of(2071D), Optional.of(0D), Optional.empty())
        .verifyComplete();

    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? 60D : 2060D),
            Optional.of(meta.isMariaDBServer() ? 71D : 1971D),
            Optional.of(0D),
            Optional.empty())
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("2060"), Optional.of("2071"), Optional.of("0000"), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? "60" : "2060"),
            Optional.of(meta.isMariaDBServer() ? "71" : "1971"),
            Optional.of(meta.isMariaDBServer() ? "00" : "0000"),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void localDateValue() {
    localDateValue(sharedConn);
  }

  @Test
  void localDateValuePrepare() {
    localDateValue(sharedConnPrepare);
  }

  private void localDateValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDate.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalDate.parse("2060-01-01")),
            Optional.of(LocalDate.parse("2071-01-01")),
            Optional.of(LocalDate.parse("0000-01-01")),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDate.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalDate.parse("2060-01-01")),
            Optional.of(LocalDate.parse("1971-01-01")),
            Optional.of(LocalDate.parse(meta.isMariaDBServer() ? "2000-01-01" : "0000-01-01")),
            Optional.empty())
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigDecimal("2060")),
            Optional.of(new BigDecimal("2071")),
            Optional.of(new BigDecimal("0000")),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigDecimal(meta.isMariaDBServer() ? "60" : "2060")),
            Optional.of(new BigDecimal(meta.isMariaDBServer() ? "71" : "1971")),
            Optional.of(new BigDecimal(meta.isMariaDBServer() ? "00" : "0000")),
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigInteger("2060")),
            Optional.of(new BigInteger("2071")),
            Optional.of(new BigInteger("0")),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigInteger(meta.isMariaDBServer() ? "60" : "2060")),
            Optional.of(new BigInteger(meta.isMariaDBServer() ? "71" : "1971")),
            Optional.of(BigInteger.ZERO),
            Optional.empty())
        .verifyComplete();
  }
}

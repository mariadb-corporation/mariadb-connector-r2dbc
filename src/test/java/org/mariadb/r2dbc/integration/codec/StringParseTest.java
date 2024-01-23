// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.codec;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.util.MariadbType;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class StringParseTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    afterAll2();
    sharedConn.beginTransaction().block();
    sharedConn.createStatement("DROP TABLE IF EXISTS StringTable").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE StringTable (t1 varchar(256), t2 TEXT) CHARACTER SET utf8mb4 COLLATE"
                + " utf8mb4_unicode_ci")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO StringTable VALUES ('some🌟', 'some🌟'),('1', '1'),('0', '0'), (null,"
                + " null)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE StringBinary (t1 varbinary(256), t2 varbinary(1024)) CHARACTER SET"
                + " utf8mb4 COLLATE utf8mb4_unicode_ci")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO StringBinary VALUES ('some🌟', 'some🌟'),('1', '1'),('0', '0'), (null,"
                + " null)")
        .execute()
        .blockLast();
    sharedConn.createStatement("FLUSH TABLES").execute().blockLast();
    sharedConn.commitTransaction().block();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS StringTable").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS StringBinary").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS durationValue").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS localTimeValue").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS localDateValue").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS localDateTimeValue").execute().blockLast();
  }

  public static byte[] hexStringToByteArray(String hex) {
    int l = hex.length();
    byte[] data = new byte[l / 2];
    for (int i = 0; i < l; i += 2) {
      data[i / 2] =
          (byte)
              ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
  }

  @Test
  void wrongType() {
    wrongType(sharedConn);
  }

  @Test
  void wrongTypePrepare() {
    wrongType(sharedConnPrepare);
  }

  private void wrongType(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, this.getClass()))))
        .as(StepVerifier::create)
        .expectError();
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("some🌟"), Optional.of("1"), Optional.of("0"), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM StringTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("some🌟"), Optional.of("1"), Optional.of("0"), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Object.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("some🌟"), Optional.of("1"), Optional.of("0"), Optional.empty())
        .verifyComplete();
  }

  @Test
  void tt() {
    String b =
        "0xFFBF0F23485930303054686520636C69656E742077617320646973636F6E6E656374656420627920746865207365727665722062656361757365206F6620696E61637469766974792E2053656520776169745F74696D656F757420616E6420696E7465726163746976655F74696D656F757420666F7220636F6E6669677572696E672074686973206265686176696F722E";
    byte[] bytes = hexStringToByteArray(b);
    String st = new String(bytes, StandardCharsets.UTF_8);
    System.out.println(st);
  }

  @Test
  void defaultValueBinary() {
    defaultValueBinary(sharedConn);
  }

  @Test
  void defaultValuePrepareBinary() {
    defaultValueBinary(sharedConnPrepare);
  }

  private void defaultValueBinary(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM StringBinary WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .consumeNextWith(
            c -> {
              Assertions.assertTrue(c.get() instanceof byte[]);
              Assertions.assertArrayEquals(
                  "some🌟".getBytes(StandardCharsets.UTF_8), (byte[]) c.get());
            })
        .consumeNextWith(
            c ->
                Assertions.assertArrayEquals(
                    "1".getBytes(StandardCharsets.UTF_8), (byte[]) c.get()))
        .consumeNextWith(
            c ->
                Assertions.assertArrayEquals(
                    "0".getBytes(StandardCharsets.UTF_8), (byte[]) c.get()))
        .expectNext(Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM StringBinary WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .consumeNextWith(
            c -> {
              Assertions.assertTrue(c.get() instanceof byte[]);
              Assertions.assertArrayEquals(
                  "some🌟".getBytes(StandardCharsets.UTF_8), (byte[]) c.get());
            })
        .consumeNextWith(
            c -> {
              Assertions.assertArrayEquals("1".getBytes(StandardCharsets.UTF_8), (byte[]) c.get());
            })
        .consumeNextWith(
            c -> {
              Assertions.assertArrayEquals("0".getBytes(StandardCharsets.UTF_8), (byte[]) c.get());
            })
        .expectNext(Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Object.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("some🌟"), Optional.of("1"), Optional.of("0"), Optional.empty())
        .verifyComplete();
  }

  @Test
  void clobValue() {
    clobValue(sharedConn);
  }

  @Test
  void clobValuePrepare() {
    clobValue(sharedConnPrepare);
  }

  private void clobValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? limit 2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Mono.from(row.get(0, Clob.class).stream())))
        .as(StepVerifier::create)
        .assertNext(
            val ->
                val.handle(
                        (ch, sink) -> {
                          if (ch.equals("some🌟")) {
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
    booleanValue(sharedConn);
  }

  @Test
  void booleanValuePrepare() {
    booleanValue(sharedConnPrepare);
  }

  private void booleanValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(true), Optional.of(true), Optional.of(false), Optional.empty())
        .verifyComplete();
  }

  @Test
  void unknownValue() {
    unknownValue(sharedConn);
  }

  @Test
  void unknownValuePrepare() {
    unknownValue(sharedConnPrepare);
  }

  private void unknownValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?  LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, this.getClass()))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "No decoder for type"
                                + " org.mariadb.r2dbc.integration.codec.StringParseTest and column"
                                + " type VARSTRING"))
        .verify();
  }

  @Test
  void durationValue() {
    durationValue(sharedConn);
  }

  @Test
  void durationValuePrepare() {
    durationValue(sharedConnPrepare);
  }

  private void durationValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?  LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Duration.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("VARSTRING value 'some\uD83C\uDF1F' cannot be decoded as Time"))
        .verify();
    sharedConn.createStatement("DROP TABLE IF EXISTS durationValue").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE durationValue (t1 varchar(256)) CHARACTER SET utf8mb4 COLLATE"
                + " utf8mb4_unicode_ci")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO durationValue VALUES ('90:00:00.012340'), ('800:00:00.123'), ('800'),"
                + " ('22'), (null)")
        .execute()
        .blockLast();

    connection
        .createStatement("SELECT t1 FROM durationValue WHERE 1 = ?  LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Duration.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(Duration.parse("P3DT18H0.012340S")),
            Optional.of(Duration.parse("P33DT8H0.123S")))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("VARSTRING value '800' cannot be decoded as Time"))
        .verify();
  }

  @Test
  void localTimeValue() {
    localTimeValue(sharedConn);
  }

  @Test
  void localTimeValuePrepare() {
    localTimeValue(sharedConnPrepare);
  }

  private void localTimeValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?  LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalTime.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "value 'some\uD83C\uDF1F' (VARSTRING) cannot be decoded as LocalTime"))
        .verify();
    sharedConn.createStatement("DROP TABLE IF EXISTS localTimeValue").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE localTimeValue (t1 varchar(256)) CHARACTER SET utf8mb4 COLLATE"
                + " utf8mb4_unicode_ci")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO localTimeValue VALUES ('18:00:00.012340'), ('08:01:18.123'), ('800'),"
                + " ('22'), (null)")
        .execute()
        .blockLast();

    connection
        .createStatement("SELECT t1 FROM localTimeValue WHERE 1 = ?  LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalTime.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalTime.parse("18:00:00.012340")),
            Optional.of(LocalTime.parse("08:01:18.123")))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value '800' (VARSTRING) cannot be decoded as LocalTime"))
        .verify();
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?  LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDate.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' (VARSTRING) cannot be decoded as Date"))
        .verify();
    sharedConn.createStatement("DROP TABLE IF EXISTS localDateValue").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE localDateValue (t1 varchar(256)) CHARACTER SET utf8mb4 COLLATE"
                + " utf8mb4_unicode_ci")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO localDateValue VALUES ('2010-01-12'), ('2011-2-28'), (null),"
                + " ('2011-a-28')")
        .execute()
        .blockLast();

    connection
        .createStatement("SELECT t1 FROM localDateValue WHERE 1 = ? LIMIT 4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDate.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalDate.parse("2010-01-12")),
            Optional.of(LocalDate.parse("2011-02-28")),
            Optional.empty())
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value '2011-a-28' (VARSTRING) cannot be decoded as Date"))
        .verify();
  }

  @Test
  void localDateTimeValue() {
    localDateTimeValue(sharedConn);
  }

  @Test
  void localDateTimeValuePrepare() {
    localDateTimeValue(sharedConnPrepare);
  }

  private void localDateTimeValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(
            r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDateTime.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "value 'some\uD83C\uDF1F' (VARSTRING) cannot be decoded as"
                                + " LocalDateTime"))
        .verify();
    sharedConn.createStatement("DROP TABLE IF EXISTS localDateTimeValue").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE localDateTimeValue (t1 varchar(256)) CHARACTER SET utf8mb4 COLLATE"
                + " utf8mb4_unicode_ci")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO localDateTimeValue VALUES ('2013-07-22 12:50:05.01230'), ('2035-01-31 "
                + "10:45:01'), (null), ('2013-07-bb 12:50:05.01230')")
        .execute()
        .blockLast();

    connection
        .createStatement("SELECT t1 FROM localDateTimeValue WHERE 1 = ? LIMIT 4")
        .bind(0, 1)
        .execute()
        .flatMap(
            r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDateTime.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalDateTime.parse("2013-07-22T12:50:05.01230")),
            Optional.of(LocalDateTime.parse("2035-01-31T10:45:01")),
            Optional.empty())
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "value '2013-07-bb 12:50:05.01230' (VARSTRING) cannot be decoded as"
                                + " LocalDateTime"))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte[].class))))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> Arrays.equals(val.get(), "some🌟".getBytes(StandardCharsets.UTF_8)))
        .expectNextMatches(val -> Arrays.equals(val.get(), "1".getBytes(StandardCharsets.UTF_8)))
        .expectNextMatches(val -> Arrays.equals(val.get(), "0".getBytes(StandardCharsets.UTF_8)))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' (VARSTRING) cannot be decoded as Byte"))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' (VARSTRING) cannot be decoded as Byte"))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' cannot be decoded as Short"))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' cannot be decoded as Integer"))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' cannot be decoded as Long"))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' cannot be decoded as Float"))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' cannot be decoded as Double"))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("some🌟"), Optional.of("1"), Optional.of("0"), Optional.empty())
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' cannot be decoded as BigDecimal"))
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("value 'some\uD83C\uDF1F' cannot be decoded as BigInteger"))
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

    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? limit 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Blob.class)))
        .cast(Blob.class)
        .flatMap(Blob::stream)
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("Data type VARSTRING (not binary) cannot be decoded as Blob"))
        .verify();
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
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getJavaType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(String.class))
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(MariadbType.VARCHAR))
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM StringTable WHERE 1 = ? LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(MariadbType.CLOB))
        .verifyComplete();
  }
}

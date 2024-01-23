// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.parameter;

import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class BigIntegerParameterTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    after2();
    sharedConn
        .createStatement("CREATE TABLE BigIntParam (t1 BIGINT, t2 BIGINT, t3 BIGINT)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS BigIntParam").execute().blockLast();
  }

  @Test
  void nullValue() {
    nullValue(sharedConn);
  }

  @Test
  void nullValuePrepare() {
    nullValue(sharedConnPrepare);
  }

  private void nullValue(MariadbConnection connection) {
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    connection
        .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
        .bindNull(0, BigInteger.class)
        .bindNull(1, BigInteger.class)
        .bindNull(2, BigInteger.class)
        .execute()
        .blockLast();
    validate(connection, Optional.empty(), Optional.empty(), Optional.empty());
    connection.rollbackTransaction().block();
  }

  @Test
  void bigIntValue() {
    bigIntValue(sharedConn);
  }

  @Test
  void bigIntValuePrepare() {
    bigIntValue(sharedConnPrepare);
  }

  private void bigIntValue(MariadbConnection connection) {
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, BigInteger.ONE)
            .bind(1, new BigInteger("9223372036854775807"))
            .bind(2, new BigInteger("-9"));
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=BigIntegerCodec}, BindValue{codec=BigIntegerCodec},"
                    + " BindValue{codec=BigIntegerCodec}]]"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(connection, Optional.of("1"), Optional.of("9223372036854775807"), Optional.of("-9"));
    connection.rollbackTransaction().block();
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
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, "1")
            .bind(1, "9223372036854775807")
            .bind(2, "-9");
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=StringCodec}, BindValue{codec=StringCodec},"
                    + " BindValue{codec=StringCodec}]]"),
        stmt.toString());

    stmt.execute().blockLast();

    validate(connection, Optional.of("1"), Optional.of("9223372036854775807"), Optional.of("-9"));
    connection.rollbackTransaction().block();
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
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, BigDecimal.ONE)
            .bind(1, new BigDecimal("9223372036854775807"))
            .bind(2, new BigDecimal("-9"));

    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=BigDecimalCodec}, BindValue{codec=BigDecimalCodec},"
                    + " BindValue{codec=BigDecimalCodec}]]"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(connection, Optional.of("1"), Optional.of("9223372036854775807"), Optional.of("-9"));
    connection.rollbackTransaction().block();
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
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, 1)
            .bind(1, -1)
            .bind(2, 0);
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=IntCodec}, BindValue{codec=IntCodec},"
                    + " BindValue{codec=IntCodec}]]"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(connection, Optional.of("1"), Optional.of("-1"), Optional.of("0"));
    connection.rollbackTransaction().block();
  }

  @Test
  void byteValue() {
    byteValue(sharedConn);
  }

  @Test
  void byteValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    byteValue(sharedConnPrepare);
  }

  private void byteValue(MariadbConnection connection) {
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, (byte) 127)
            .bind(1, (byte) -128)
            .bind(2, (byte) 0);

    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=ByteCodec}, BindValue{codec=ByteCodec},"
                    + " BindValue{codec=ByteCodec}]]"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(connection, Optional.of("127"), Optional.of("-128"), Optional.of("0"));
    connection.rollbackTransaction().block();
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
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, 127f)
            .bind(1, -128f)
            .bind(2, 0f);
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=FloatCodec}, BindValue{codec=FloatCodec},"
                    + " BindValue{codec=FloatCodec}]]"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(connection, Optional.of("127"), Optional.of("-128"), Optional.of("0"));
    connection.rollbackTransaction().block();
  }

  @Test
  void doubleValue() {
    doubleValue(sharedConn);
  }

  @Test
  void doubleValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    doubleValue(sharedConnPrepare);
  }

  private void doubleValue(MariadbConnection connection) {
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();

    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, 127d)
            .bind(1, -128d)
            .bind(2, 0d);

    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=DoubleCodec}, BindValue{codec=DoubleCodec},"
                    + " BindValue{codec=DoubleCodec}]]"),
        stmt.toString());
    stmt.execute().blockLast();
    validate(connection, Optional.of("127"), Optional.of("-128"), Optional.of("0"));
    connection.rollbackTransaction().block();
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
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, Short.valueOf("1"))
            .bind(1, Short.valueOf("-1"))
            .bind(2, Short.valueOf("0"));
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=ShortCodec}, BindValue{codec=ShortCodec},"
                    + " BindValue{codec=ShortCodec}]]"),
        stmt.toString());
    stmt.execute().blockLast();
    validate(connection, Optional.of("1"), Optional.of("-1"), Optional.of("0"));
    connection.rollbackTransaction().block();
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
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, 1L)
            .bind(1, -1L)
            .bind(2, 0L);
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=LongCodec}, BindValue{codec=LongCodec},"
                    + " BindValue{codec=LongCodec}]]"),
        stmt.toString());
    stmt.execute().blockLast();
    validate(connection, Optional.of("1"), Optional.of("-1"), Optional.of("0"));
    connection.rollbackTransaction().block();
  }

  @Test
  void LongValue() {
    LongValue(sharedConn);
  }

  @Test
  void LongValuePrepare() {
    LongValue(sharedConnPrepare);
  }

  private void LongValue(MariadbConnection connection) {
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, Long.valueOf("1"))
            .bind(1, Long.valueOf("-1"))
            .bind(2, Long.valueOf("0"));
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=LongCodec}, BindValue{codec=LongCodec},"
                    + " BindValue{codec=LongCodec}]]"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(connection, Optional.of("1"), Optional.of("-1"), Optional.of("0"));
    connection.rollbackTransaction().block();
  }

  @Test
  void localDateTimeValue() {
    Assumptions.assumeFalse(isXpand());
    localDateTimeValue(sharedConn);
  }

  private void localDateTimeValue(MariadbConnection connection) {
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, LocalDateTime.now())
            .bind(1, LocalDateTime.now())
            .bind(2, LocalDateTime.now());
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=LocalDateTimeCodec},"
                    + " BindValue{codec=LocalDateTimeCodec},"
                    + " BindValue{codec=LocalDateTimeCodec}]]"),
        stmt.toString());
    stmt.execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
    connection.rollbackTransaction().block();
  }

  @Test
  void localDateTimeValuePrepare() {
    sharedConnPrepare.beginTransaction().block();
    sharedConnPrepare.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    sharedConnPrepare
        .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.now())
        .bind(1, LocalDateTime.now())
        .bind(2, LocalDateTime.now())
        .execute()
        .blockLast();
    sharedConnPrepare.rollbackTransaction().block();
  }

  @Test
  void localDateValue() {
    Assumptions.assumeFalse(isXpand());
    localDateValue(sharedConn);
  }

  @Test
  void localDateValuePrepare() {
    sharedConnPrepare.beginTransaction().block();
    sharedConnPrepare.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    sharedConnPrepare
        .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
        .bind(0, LocalDate.now())
        .bind(1, LocalDate.now())
        .bind(2, LocalDate.now())
        .execute()
        .blockLast();
    sharedConnPrepare.rollbackTransaction().block();
  }

  private void localDateValue(MariadbConnection connection) {
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, LocalDate.now())
            .bind(1, LocalDate.now())
            .bind(2, LocalDate.now());

    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=LocalDateCodec}, BindValue{codec=LocalDateCodec},"
                    + " BindValue{codec=LocalDateCodec}]]"),
        stmt.toString());
    stmt.execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
    connection.rollbackTransaction().block();
  }

  @Test
  void localTimeValue() {
    Assumptions.assumeFalse(isXpand());
    localTimeValue(sharedConn);
  }

  @Test
  void localTimeValuePrepare() {
    sharedConnPrepare.beginTransaction().block();
    sharedConnPrepare.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    MariadbStatement stmt =
        sharedConnPrepare
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, LocalTime.now())
            .bind(1, LocalTime.now())
            .bind(2, LocalTime.now());
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=LocalTimeCodec}, BindValue{codec=LocalTimeCodec},"
                    + " BindValue{codec=LocalTimeCodec}]]"),
        stmt.toString());

    stmt.execute().blockLast();
    sharedConnPrepare.rollbackTransaction().block();
  }

  private void localTimeValue(MariadbConnection connection) {
    connection.beginTransaction().block();
    connection.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
    connection
        .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
        .bind(0, LocalTime.now())
        .bind(1, LocalTime.now())
        .bind(2, LocalTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
    connection.rollbackTransaction().block();
  }

  private void validate(
      MariadbConnection connection, Optional<String> t1, Optional<String> t2, Optional<String> t3) {
    connection
        .createStatement("SELECT * FROM BigIntParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Object obj0 = row.get(0);
                      Object obj1 = row.get(1);
                      Object obj2 = row.get(2);
                      return Flux.just(
                          Optional.ofNullable(obj0 == null ? null : obj0.toString()),
                          Optional.ofNullable(obj1 == null ? null : obj1.toString()),
                          Optional.ofNullable(obj2 == null ? null : obj2.toString()));
                    }))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}

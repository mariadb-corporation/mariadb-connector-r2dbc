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
    sharedConn
        .createStatement("CREATE TABLE BigIntParam (t1 BIGINT, t2 BIGINT, t3 BIGINT)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE BigIntParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE BigIntParam").execute().blockLast();
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
    connection
        .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
        .bindNull(0, BigInteger.class)
        .bindNull(1, BigInteger.class)
        .bindNull(2, BigInteger.class)
        .execute()
        .blockLast();
    validate(Optional.empty(), Optional.empty(), Optional.empty());
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, BigInteger.ONE)
            .bind(1, new BigInteger("9223372036854775807"))
            .bind(2, new BigInteger("-9"));
    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.math.BigInteger}, codec=BigIntegerCodec}, ParameterWithCodec{param=In{Inferred: java.math.BigInteger}, codec=BigIntegerCodec}, ParameterWithCodec{param=In{Inferred: java.math.BigInteger}, codec=BigIntegerCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.math.BigInteger}, codec=BigIntegerCodec}, 1=ParameterWithCodec{param=In{Inferred: java.math.BigInteger}, codec=BigIntegerCodec}, 2=ParameterWithCodec{param=In{Inferred: java.math.BigInteger}, codec=BigIntegerCodec}}"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(Optional.of("1"), Optional.of("9223372036854775807"), Optional.of("-9"));
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, "1")
            .bind(1, "9223372036854775807")
            .bind(2, "-9");
    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.lang.String}, codec=StringCodec}, ParameterWithCodec{param=In{Inferred: java.lang.String}, codec=StringCodec}, ParameterWithCodec{param=In{Inferred: java.lang.String}, codec=StringCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.lang.String}, codec=StringCodec}, 1=ParameterWithCodec{param=In{Inferred: java.lang.String}, codec=StringCodec}, 2=ParameterWithCodec{param=In{Inferred: java.lang.String}, codec=StringCodec}}"),
        stmt.toString());

    stmt.execute().blockLast();

    validate(Optional.of("1"), Optional.of("9223372036854775807"), Optional.of("-9"));
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, BigDecimal.ONE)
            .bind(1, new BigDecimal("9223372036854775807"))
            .bind(2, new BigDecimal("-9"));

    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.math.BigDecimal}, codec=BigDecimalCodec}, ParameterWithCodec{param=In{Inferred: java.math.BigDecimal}, codec=BigDecimalCodec}, ParameterWithCodec{param=In{Inferred: java.math.BigDecimal}, codec=BigDecimalCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.math.BigDecimal}, codec=BigDecimalCodec}, 1=ParameterWithCodec{param=In{Inferred: java.math.BigDecimal}, codec=BigDecimalCodec}, 2=ParameterWithCodec{param=In{Inferred: java.math.BigDecimal}, codec=BigDecimalCodec}}"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(Optional.of("1"), Optional.of("9223372036854775807"), Optional.of("-9"));
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, 1)
            .bind(1, -1)
            .bind(2, 0);
    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.lang.Integer}, codec=IntCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Integer}, codec=IntCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Integer}, codec=IntCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.lang.Integer}, codec=IntCodec}, 1=ParameterWithCodec{param=In{Inferred: java.lang.Integer}, codec=IntCodec}, 2=ParameterWithCodec{param=In{Inferred: java.lang.Integer}, codec=IntCodec}}"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(Optional.of("1"), Optional.of("-1"), Optional.of("0"));
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, (byte) 127)
            .bind(1, (byte) -128)
            .bind(2, (byte) 0);

    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.lang.Byte}, codec=ByteCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Byte}, codec=ByteCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Byte}, codec=ByteCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.lang.Byte}, codec=ByteCodec}, 1=ParameterWithCodec{param=In{Inferred: java.lang.Byte}, codec=ByteCodec}, 2=ParameterWithCodec{param=In{Inferred: java.lang.Byte}, codec=ByteCodec}}"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(Optional.of("127"), Optional.of("-128"), Optional.of("0"));
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, 127f)
            .bind(1, -128f)
            .bind(2, 0f);
    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.lang.Float}, codec=FloatCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Float}, codec=FloatCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Float}, codec=FloatCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.lang.Float}, codec=FloatCodec}, 1=ParameterWithCodec{param=In{Inferred: java.lang.Float}, codec=FloatCodec}, 2=ParameterWithCodec{param=In{Inferred: java.lang.Float}, codec=FloatCodec}}"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(Optional.of("127"), Optional.of("-128"), Optional.of("0"));
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, 127d)
            .bind(1, -128d)
            .bind(2, 0d);

    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.lang.Double}, codec=DoubleCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Double}, codec=DoubleCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Double}, codec=DoubleCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.lang.Double}, codec=DoubleCodec}, 1=ParameterWithCodec{param=In{Inferred: java.lang.Double}, codec=DoubleCodec}, 2=ParameterWithCodec{param=In{Inferred: java.lang.Double}, codec=DoubleCodec}}"),
        stmt.toString());
    stmt.execute().blockLast();
    validate(Optional.of("127"), Optional.of("-128"), Optional.of("0"));
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, Short.valueOf("1"))
            .bind(1, Short.valueOf("-1"))
            .bind(2, Short.valueOf("0"));
    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.lang.Short}, codec=ShortCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Short}, codec=ShortCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Short}, codec=ShortCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.lang.Short}, codec=ShortCodec}, 1=ParameterWithCodec{param=In{Inferred: java.lang.Short}, codec=ShortCodec}, 2=ParameterWithCodec{param=In{Inferred: java.lang.Short}, codec=ShortCodec}}"),
        stmt.toString());
    stmt.execute().blockLast();
    validate(Optional.of("1"), Optional.of("-1"), Optional.of("0"));
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, 1L)
            .bind(1, -1L)
            .bind(2, 0L);
    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}, 1=ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}, 2=ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}}"),
        stmt.toString());
    stmt.execute().blockLast();
    validate(Optional.of("1"), Optional.of("-1"), Optional.of("0"));
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
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, Long.valueOf("1"))
            .bind(1, Long.valueOf("-1"))
            .bind(2, Long.valueOf("0"));
    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters=[ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}, ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}]")
            || stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}, 1=ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}, 2=ParameterWithCodec{param=In{Inferred: java.lang.Long}, codec=LongCodec}}"),
        stmt.toString());

    stmt.execute().blockLast();
    validate(Optional.of("1"), Optional.of("-1"), Optional.of("0"));
  }

  @Test
  void localDateTimeValue() {
    localDateTimeValue(sharedConn);
  }

  private void localDateTimeValue(MariadbConnection connection) {
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, LocalDateTime.now())
            .bind(1, LocalDateTime.now())
            .bind(2, LocalDateTime.now());
    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "parameters=[ParameterWithCodec{param=In{Inferred: java.time.LocalDateTime}, codec=LocalDateTimeCodec}, ParameterWithCodec{param=In{Inferred: java.time.LocalDateTime}, codec=LocalDateTimeCodec}, ParameterWithCodec{param=In{Inferred: java.time.LocalDateTime}, codec=LocalDateTimeCodec}]"),
        stmt.toString());
    stmt.execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
  }

  @Test
  void localDateTimeValuePrepare() {
    sharedConnPrepare
        .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.now())
        .bind(1, LocalDateTime.now())
        .bind(2, LocalDateTime.now())
        .execute()
        .blockLast();
  }

  @Test
  void localDateValue() {
    localDateValue(sharedConn);
  }

  @Test
  void localDateValuePrepare() {
    sharedConnPrepare
        .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
        .bind(0, LocalDate.now())
        .bind(1, LocalDate.now())
        .bind(2, LocalDate.now())
        .execute()
        .blockLast();
  }

  private void localDateValue(MariadbConnection connection) {
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, LocalDate.now())
            .bind(1, LocalDate.now())
            .bind(2, LocalDate.now());

    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "parameters=[ParameterWithCodec{param=In{Inferred: java.time.LocalDate}, codec=LocalDateCodec}, ParameterWithCodec{param=In{Inferred: java.time.LocalDate}, codec=LocalDateCodec}, ParameterWithCodec{param=In{Inferred: java.time.LocalDate}, codec=LocalDateCodec}]"),
        stmt.toString());
    stmt.execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
  }

  @Test
  void localTimeValue() {
    localTimeValue(sharedConn);
  }

  @Test
  void localTimeValuePrepare() {
    MariadbStatement stmt =
        sharedConnPrepare
            .createStatement("INSERT INTO BigIntParam VALUES (?,?,?)")
            .bind(0, LocalTime.now())
            .bind(1, LocalTime.now())
            .bind(2, LocalTime.now());
    Assertions.assertTrue(
        stmt.toString()
                .contains(
                    "parameters={0=ParameterWithCodec{param=In{Inferred: java.time.LocalTime}, codec=LocalTimeCodec}, 1=ParameterWithCodec{param=In{Inferred: java.time.LocalTime}, codec=LocalTimeCodec}, 2=ParameterWithCodec{param=In{Inferred: java.time.LocalTime}, codec=LocalTimeCodec}}")
            || stmt.toString()
                .contains(
                    "parameters={0=In{Inferred: java.time.LocalTime}, 1=In{Inferred: java.time.LocalTime}, 2=In{Inferred: java.time.LocalTime}}"),
        stmt.toString());

    stmt.execute().blockLast();
  }

  private void localTimeValue(MariadbConnection connection) {
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
  }

  private void validate(Optional<String> t1, Optional<String> t2, Optional<String> t3) {
    sharedConn
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

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.parameter;

import io.r2dbc.spi.R2dbcBadGrammarException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbResult;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class TimeParameterTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TABLE TimeParam (t1 TIME(6), t2 TIME(6), t3 TIME(6))")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE TimeParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE TimeParam").execute().blockLast();
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
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bindNull(0, LocalTime.class)
        .bindNull(1, LocalTime.class)
        .bindNull(2, LocalTime.class)
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
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
            .bind(0, BigInteger.ONE)
            .bind(1, new BigInteger("9223372036854775807"))
            .bind(2, new BigInteger("-9"))
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcBadGrammarException
                      && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
          .verify();
    }
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
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
            .bind(0, "1")
            .bind(1, "9223372036854775807")
            .bind(2, "-9")
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcBadGrammarException
                      && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
          .verify();
    }
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
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
            .bind(0, BigDecimal.ONE)
            .bind(1, new BigDecimal("9223372036854775807"))
            .bind(2, new BigDecimal("-9"))
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcBadGrammarException
                      && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
          .verify();
    }
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
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, 1)
        .bind(1, -1)
        .bind(2, 0)
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("00:00:01")),
        Optional.of(LocalTime.parse("23:59:59")),
        Optional.of(LocalTime.parse("00:00:00")));
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
    connection
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, (byte) 127)
        .bind(1, (byte) -128)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("00:01:27")),
        Optional.of(LocalTime.parse("23:58:32")),
        Optional.of(LocalTime.parse("00:00:00")));
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
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, 127f)
        .bind(1, -128f)
        .bind(2, 0f)
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("00:01:27")),
        Optional.of(LocalTime.parse("23:58:32")),
        Optional.of(LocalTime.parse("00:00:00")));
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
    connection
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, 127d)
        .bind(1, 128d)
        .bind(2, 0d)
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("00:01:27")),
        Optional.of(LocalTime.parse("00:01:28")),
        Optional.of(LocalTime.parse("00:00:00")));
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
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("1"))
        .bind(1, Short.valueOf("-1"))
        .bind(2, Short.valueOf("0"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("00:00:01")),
        Optional.of(LocalTime.parse("23:59:59")),
        Optional.of(LocalTime.parse("00:00:00")));
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
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, Long.valueOf("1"))
        .bind(1, Long.valueOf("-1"))
        .bind(2, Long.valueOf("0"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("00:00:01")),
        Optional.of(LocalTime.parse("23:59:59")),
        Optional.of(LocalTime.parse("00:00:00")));
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
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.parse("2010-01-12T05:08:09.0014"))
        .bind(1, LocalDateTime.parse("2018-12-15T05:08:10.123456"))
        .bind(2, LocalDateTime.parse("2025-05-12T05:08:11.123"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("05:08:09.0014")),
        Optional.of(LocalTime.parse("05:08:10.123456")),
        Optional.of(LocalTime.parse("05:08:11.123")));
  }

  @Test
  void localDateValue() {
    localDateValue(sharedConn);
  }

  @Test
  void localDateValuePrepare() {
    sharedConnPrepare
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, LocalDate.parse("2010-01-12"))
        .bind(1, LocalDate.parse("2018-12-15"))
        .bind(2, LocalDate.parse("2025-05-12"))
        .execute()
        .blockLast();
    sharedConn
        .createStatement("SELECT * FROM TimeParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            row.get(0, String.class),
                            row.get(1, String.class),
                            row.get(2, String.class))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext("00:00:00.000000", "00:00:00.000000", "00:00:00.000000")
        .verifyComplete();
  }

  private void localDateValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, LocalDate.parse("2010-01-12"))
        .bind(1, LocalDate.parse("2018-12-15"))
        .bind(2, LocalDate.parse("2025-05-12"))
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22007"))
        .verify();
  }

  @Test
  void durationValue() {
    durationValue(sharedConn);
    durationValue2(sharedConn);
  }

  @Test
  void durationValuePrepare() {
    durationValue(sharedConnPrepare);
    durationValue2(sharedConnPrepare);
  }

  private void durationValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, Duration.parse("PT5H8M9.0014S"))
        .bind(1, Duration.parse("PT-5H-8M-10S"))
        .bind(2, Duration.parse("PT5H8M11.123S"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("05:08:09.0014")),
        Optional.of(LocalTime.parse("18:51:50")),
        Optional.of(LocalTime.parse("05:08:11.123")));
  }

  private void durationValue2(MariadbConnection connection) {
    connection.createStatement("TRUNCATE TABLE TimeParam").execute().blockLast();

    connection
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, Duration.parse("PT0S"))
        .bind(1, Duration.parse("PT-1.123S"))
        .bind(2, Duration.parse("PT-5H-8M-11.123S"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("00:00:00")),
        Optional.of(LocalTime.parse("23:59:58.877")),
        Optional.of(LocalTime.parse("18:51:48.877")));
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
        .createStatement("INSERT INTO TimeParam VALUES (?,?,?)")
        .bind(0, LocalTime.parse("05:08:09.0014"))
        .bind(1, LocalTime.parse("05:08:10"))
        .bind(2, LocalTime.parse("05:08:11.123"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalTime.parse("05:08:09.0014")),
        Optional.of(LocalTime.parse("05:08:10")),
        Optional.of(LocalTime.parse("05:08:11.123")));
  }

  private void validate(Optional<LocalTime> t1, Optional<LocalTime> t2, Optional<LocalTime> t3) {
    sharedConn
        .createStatement("SELECT * FROM TimeParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            Optional.ofNullable((LocalTime) row.get(0)),
                            Optional.ofNullable(row.get(1)),
                            Optional.ofNullable(row.get(2)))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}

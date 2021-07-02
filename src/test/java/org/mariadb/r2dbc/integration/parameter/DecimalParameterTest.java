// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.parameter;

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class DecimalParameterTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement(
            "CREATE TABLE DecimalParam (t1 DECIMAL(40,20), t2 DECIMAL(40,20), t3 DECIMAL(40,20))")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE DecimalParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE DecimalParam").execute().blockLast();
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
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bindNull(0, BigDecimal.class)
        .bindNull(1, BigDecimal.class)
        .bindNull(2, BigDecimal.class)
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
    connection
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, BigDecimal.ONE)
        .bind(1, new BigInteger("9223372036854775807"))
        .bind(2, new BigInteger("-9"))
        .execute()
        .blockLast();
    validate(
        Optional.of(new BigDecimal("1.00000000000000000000")),
        Optional.of(new BigDecimal("9223372036854775807.00000000000000000000")),
        Optional.of(new BigDecimal("-9.00000000000000000000")));
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
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, "1")
        .bind(1, "9223372036854775807.0456")
        .bind(2, "-9")
        .execute()
        .blockLast();
    validate(
        Optional.of(new BigDecimal("1.00000000000000000000")),
        Optional.of(new BigDecimal("9223372036854775807.04560000000000000000")),
        Optional.of(new BigDecimal("-9.00000000000000000000")));
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
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, BigDecimal.ONE)
        .bind(1, new BigDecimal("9223372036854775807.0456"))
        .bind(2, new BigDecimal("-9"))
        .execute()
        .blockLast();
    validate(
        Optional.of(new BigDecimal("1.00000000000000000000")),
        Optional.of(new BigDecimal("9223372036854775807.04560000000000000000")),
        Optional.of(new BigDecimal("-9.00000000000000000000")));
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
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, 1)
        .bind(1, -1)
        .bind(2, 0)
        .execute()
        .blockLast();
    validate(
        Optional.of(new BigDecimal("1.00000000000000000000")),
        Optional.of(new BigDecimal("-1.00000000000000000000")),
        Optional.of(new BigDecimal("0.00000000000000000000")));
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
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, (byte) 127)
        .bind(1, (byte) -128)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validate(
        Optional.of(new BigDecimal("127.00000000000000000000")),
        Optional.of(new BigDecimal("-128.00000000000000000000")),
        Optional.of(new BigDecimal("0.00000000000000000000")));
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
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, 127f)
        .bind(1, -128.5f)
        .bind(2, 0f)
        .execute()
        .blockLast();
    validate(
        Optional.of(new BigDecimal("127.00000000000000000000")),
        Optional.of(new BigDecimal("-128.50000000000000000000")),
        Optional.of(new BigDecimal("0.00000000000000000000")));
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
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, 127d)
        .bind(1, -128.456d)
        .bind(2, 0d)
        .execute()
        .blockLast();
    validate(
        Optional.of(new BigDecimal("127.00000000000000000000")),
        Optional.of(new BigDecimal("-128.45600000000000000000")),
        Optional.of(new BigDecimal("0.00000000000000000000")));
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
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("1"))
        .bind(1, Short.valueOf("-1"))
        .bind(2, Short.valueOf("0"))
        .execute()
        .blockLast();
    validate(
        Optional.of(new BigDecimal("1.00000000000000000000")),
        Optional.of(new BigDecimal("-1.00000000000000000000")),
        Optional.of(new BigDecimal("0.00000000000000000000")));
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
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, Long.valueOf("1"))
        .bind(1, Long.valueOf("-1"))
        .bind(2, Long.valueOf("0"))
        .execute()
        .blockLast();
    validate(
        Optional.of(new BigDecimal("1.00000000000000000000")),
        Optional.of(new BigDecimal("-1.00000000000000000000")),
        Optional.of(new BigDecimal("0.00000000000000000000")));
  }

  @Test
  void localDateTimeValue() {
    localDateTimeValue(sharedConn);
  }

  @Test
  void localDateTimeValuePrepare() {
    sharedConnPrepare
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.now())
        .bind(1, LocalDateTime.now())
        .bind(2, LocalDateTime.now())
        .execute()
        .blockLast();
  }

  private void localDateTimeValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.now())
        .bind(1, LocalDateTime.now())
        .bind(2, LocalDateTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                (throwable instanceof R2dbcTransientResourceException
                        || throwable instanceof R2dbcBadGrammarException)
                    && Arrays.asList(new String[] {"01000", "22007", "HY000"})
                        .contains(((R2dbcException) throwable).getSqlState()))
        .verify();
  }

  @Test
  void localDateValue() {
    localDateValue(sharedConn);
  }

  @Test
  void localDateValuePrepare() {
    sharedConnPrepare
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, LocalDate.now())
        .bind(1, LocalDate.now())
        .bind(2, LocalDate.now())
        .execute()
        .blockLast();
  }

  private void localDateValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, LocalDate.now())
        .bind(1, LocalDate.now())
        .bind(2, LocalDate.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                (throwable instanceof R2dbcTransientResourceException
                        || throwable instanceof R2dbcBadGrammarException)
                    && Arrays.asList(new String[] {"01000", "22007", "HY000"})
                        .contains(((R2dbcException) throwable).getSqlState()))
        .verify();
  }

  @Test
  void localTimeValue() {
    localTimeValue(sharedConn);
  }

  @Test
  void localTimeValuePrepare() {
    sharedConnPrepare
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, LocalTime.now())
        .bind(1, LocalTime.now())
        .bind(2, LocalTime.now())
        .execute()
        .blockLast();
  }

  private void localTimeValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO DecimalParam VALUES (?,?,?)")
        .bind(0, LocalTime.now())
        .bind(1, LocalTime.now())
        .bind(2, LocalTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                (throwable instanceof R2dbcTransientResourceException
                        || throwable instanceof R2dbcBadGrammarException)
                    && Arrays.asList(new String[] {"01000", "22007", "HY000"})
                        .contains(((R2dbcException) throwable).getSqlState()))
        .verify();
  }

  private void validate(Optional<BigDecimal> t1, Optional<BigDecimal> t2, Optional<BigDecimal> t3) {
    sharedConn
        .createStatement("SELECT * FROM DecimalParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            Optional.ofNullable((BigDecimal) row.get(0)),
                            Optional.ofNullable(row.get(1)),
                            Optional.ofNullable(row.get(2)))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.parameter;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.R2dbcBadGrammarException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Optional;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbResult;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class BitParameterTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TABLE ByteParam (t1 BIT(4), t2 BIT(20), t3 BIT(2))")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE ByteParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE ByteParam").execute().blockLast();
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bindNull(0, Byte.class)
        .bindNull(1, Byte.class)
        .bindNull(2, Byte.class)
        .execute()
        .blockLast();
    validate(Optional.empty(), Optional.empty(), Optional.empty());
  }

  @Test
  void booleanValue() {
    booleanValue(sharedConn);
  }

  @Test
  void booleanValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    booleanValue(sharedConnPrepare);
  }

  private void booleanValue(MariadbConnection connection) {
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
            .bind(0, Boolean.TRUE)
            .bind(1, Boolean.TRUE)
            .bind(2, Boolean.FALSE);

    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[Binding{binds={0=BindValue{codec=BooleanCodec}, 1=BindValue{codec=BooleanCodec}, 2=BindValue{codec=BooleanCodec}}}]"),
        stmt.toString());
    stmt.execute().blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));
  }

  @Test
  void bigIntValue() {
    bigIntValue(sharedConn);
  }

  @Test
  void bigIntValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    bigIntValue(sharedConnPrepare);
  }

  private void bigIntValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, new BigInteger("11"))
        .bind(1, new BigInteger("512"))
        .bind(2, new BigInteger("1"))
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, "\1")
        .bind(1, "A")
        .bind(2, "\0")
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 65, 0, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));
  }

  @Test
  void decimalValue() {
    decimalValue(sharedConn);
  }

  @Test
  void decimalValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    decimalValue(sharedConnPrepare);
  }

  private void decimalValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, new BigDecimal("11"))
        .bind(1, new BigDecimal("512"))
        .bind(2, new BigDecimal("1"))
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
  }

  @Test
  void intValue() {
    intValue(sharedConn);
  }

  @Test
  void intValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    intValue(sharedConnPrepare);
  }

  private void intValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, 11)
        .bind(1, 512)
        .bind(2, 1)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, (byte) 15)
        .bind(1, (byte) 127)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 15})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 127, 0, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 15}))))
        .bind(1, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 1, 2}))))
        .bind(2, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {0}))))
        .execute()
        .blockLast();

    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 15})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 2, (byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));

    sharedConn.createStatement("TRUNCATE TABLE ByteParam").execute().blockLast();

    connection
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 15}))))
        .bind(1, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 1, 2}))))
        .bind(2, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {0}))))
        .execute()
        .blockLast();

    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 15})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 2, (byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, 11f)
        .bind(1, 512f)
        .bind(2, 1f)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, 11d)
        .bind(1, 512d)
        .bind(2, 1d)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("11"))
        .bind(1, Short.valueOf("127"))
        .bind(2, Short.valueOf("1"))
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 127, 0, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
  }

  @Test
  void longValue() {
    longValue(sharedConn);
  }

  @Test
  void longValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    longValue(sharedConnPrepare);
  }

  private void longValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, 11L)
        .bind(1, 512L)
        .bind(2, 1L)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
            .bind(0, LocalDateTime.now())
            .bind(1, LocalDateTime.now())
            .bind(2, LocalDateTime.now())
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
                      && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22001"))
          .verify();
    }
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
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
            .bind(0, LocalDate.now())
            .bind(1, LocalDate.now())
            .bind(2, LocalDate.now())
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
                      && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22001"))
          .verify();
    }
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
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
            .bind(0, LocalTime.now())
            .bind(1, LocalTime.now())
            .bind(2, LocalTime.now())
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
                      && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22001"))
          .verify();
    }
  }

  private void validate(Optional<BitSet> t1, Optional<BitSet> t2, Optional<BitSet> t3) {
    sharedConn
        .createStatement("SELECT * FROM ByteParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      BitSet obj0 = (BitSet) row.get(0);
                      BitSet obj1 = (BitSet) row.get(1);
                      BitSet obj2 = (BitSet) row.get(2);
                      return Flux.just(
                          Optional.ofNullable(obj0),
                          Optional.ofNullable(obj1),
                          Optional.ofNullable(obj2));
                    }))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}

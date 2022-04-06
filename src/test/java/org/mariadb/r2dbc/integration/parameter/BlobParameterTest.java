// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.parameter;

import io.r2dbc.spi.Blob;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class BlobParameterTest extends BaseConnectionTest {
  private static final MariadbConnectionMetadata meta = sharedConn.getMetadata();

  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TABLE BlobParam (t1 BLOB, t2 BLOB, t3 BLOB)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE BlobParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE BlobParam").execute().blockLast();
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
    bigIntValue(sharedConn);
  }

  @Test
  void bigIntValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    bigIntValue(sharedConnPrepare);
  }

  private void bigIntValue(MariadbConnection connection) {
    connection
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
    stringValue(sharedConn);
  }

  @Test
  void stringValuePrepare() {
    stringValue(sharedConnPrepare);
  }

  private void stringValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, "\1")
        .bind(1, "A")
        .bind(2, "ô\0你好\\")
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("\1".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("A".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("ô\0你好\\".getBytes(StandardCharsets.UTF_8)));
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
    intValue(sharedConn);
  }

  @Test
  void intValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    intValue(sharedConnPrepare);
  }

  private void intValue(MariadbConnection connection) {
    connection
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
    byteValue(sharedConn);
  }

  @Test
  void byteValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    byteValue(sharedConnPrepare);
  }

  private void byteValue(MariadbConnection connection) {
    connection
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
    blobValue(sharedConn);
  }

  @Test
  void blobValuePrepare() {
    blobValue(sharedConnPrepare);
  }

  private void blobValue(MariadbConnection connection) {
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
            .bind(0, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 15}))))
            .bind(
                1,
                Blob.from(
                    Mono.just(ByteBuffer.wrap(new byte[] {(byte) 1, 0, (byte) 127, (byte) 92}))))
            .bind(2, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {0}))));
    stmt.execute().blockLast();
    validateNotNull(
        ByteBuffer.wrap(new byte[] {(byte) 15}),
        ByteBuffer.wrap(new byte[] {(byte) 1, 0, (byte) 127, (byte) 92}),
        ByteBuffer.wrap(new byte[] {0}));
  }

  @Test
  void streamValue() {
    streamValue(sharedConn);
  }

  @Test
  void streamValuePrepare() {
    streamValue(sharedConnPrepare);
  }

  private void streamValue(MariadbConnection connection) {
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
            .bind(0, new ByteArrayInputStream(new byte[] {(byte) 15}))
            .bind(1, new ByteArrayInputStream(new byte[] {(byte) 1, 0, (byte) 127}))
            .bind(2, new ByteArrayInputStream(new byte[] {0}));
    stmt.execute().blockLast();
    validateNotNull(
        ByteBuffer.wrap(new byte[] {(byte) 15}),
        ByteBuffer.wrap(new byte[] {(byte) 1, 0, (byte) 127}),
        ByteBuffer.wrap(new byte[] {0}));
  }

  @Test
  void inputStreamValue() {
    inputStreamValue(sharedConn);
  }

  @Test
  void inputStreamValuePrepare() {
    inputStreamValue(sharedConnPrepare);
  }

  @Test
  void inputStreamValueNoBackslash() throws Exception {
    Map<String, String> sessionMap = new HashMap<>();
    sessionMap.put("SQL_MODE", "NO_BACKSLASH_ESCAPES");
    MariadbConnectionConfiguration confNoBackSlash =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionMap).build();
    MariadbConnection con = new MariadbConnectionFactory(confNoBackSlash).create().block();
    inputStreamValue(con);
    con.close().block();
  }

  @Test
  void inputStreamValueNoBackslashPrepare() throws Exception {
    Map<String, String> sessionMap = new HashMap<>();
    sessionMap.put("SQL_MODE", "NO_BACKSLASH_ESCAPES");
    MariadbConnectionConfiguration confNoBackSlash =
        TestConfiguration.defaultBuilder
            .clone()
            .sessionVariables(sessionMap)
            .useServerPrepStmts(true)
            .build();
    MariadbConnection con = new MariadbConnectionFactory(confNoBackSlash).create().block();
    inputStreamValue(con);
    con.close().block();
  }

  private void inputStreamValue(MariadbConnection connection) {
    MariadbStatement stmt =
        connection
            .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
            .bind(0, new ByteArrayInputStream(new byte[] {(byte) 15}))
            .bind(1, new ByteArrayInputStream((new byte[] {(byte) 1, 39, (byte) 127})))
            .bind(2, new ByteArrayInputStream((new byte[] {0})));
    stmt.execute().blockLast();
    validateNotNull(
        ByteBuffer.wrap(new byte[] {(byte) 15}),
        ByteBuffer.wrap(new byte[] {(byte) 1, 39, (byte) 127}),
        ByteBuffer.wrap(new byte[] {0}));
  }

  @Test
  void floatValue() {
    floatValue(sharedConn);
    validateNotNull(
        ByteBuffer.wrap("11.0".getBytes()),
        ByteBuffer.wrap("512.0".getBytes()),
        ByteBuffer.wrap("1.0".getBytes()));
  }

  @Test
  void floatValuePrepare() {
    floatValue(sharedConnPrepare);
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  private void floatValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11f)
        .bind(1, 512f)
        .bind(2, 1f)
        .execute()
        .blockLast();
  }

  @Test
  void doubleValue() {
    doubleValue(sharedConn);
    validateNotNull(
        ByteBuffer.wrap("11.0".getBytes()),
        ByteBuffer.wrap("512.0".getBytes()),
        ByteBuffer.wrap("1.0".getBytes()));
  }

  @Test
  void doubleValuePrepare() {
    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    doubleValue(sharedConnPrepare);
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  private void doubleValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11d)
        .bind(1, 512d)
        .bind(2, 1d)
        .execute()
        .blockLast();
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
    longValue(sharedConn);
  }

  @Test
  void longValuePrepare() {
    longValue(sharedConnPrepare);
  }

  private void longValue(MariadbConnection connection) {
    connection
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
    localDateTimeValue(sharedConn, "2013-07-22 12:50:05.012300", "2025-01-31 10:45:01.123000");
  }

  @Test
  void localDateTimeValuePrepare() {

    localDateTimeValue(
        sharedConnPrepare,
        meta.isMariaDBServer() ? "2013-07-22 12:50:05.012300" : "2013-07-22 12:50:05",
        meta.isMariaDBServer() ? "2025-01-31 10:45:01.123000" : "2025-01-31 10:45:01");
  }

  private void localDateTimeValue(MariadbConnection connection, String t1, String t3) {
    connection
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.parse("2013-07-22T12:50:05.01230"))
        .bind(1, LocalDateTime.parse("2035-01-31T10:45:01"))
        .bind(2, LocalDateTime.parse("2025-01-31T10:45:01.123"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap(t1.getBytes()),
        ByteBuffer.wrap("2035-01-31 10:45:01".getBytes()),
        ByteBuffer.wrap(t3.getBytes()));
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
    localTimeValue(sharedConn);
    validateNotNull(
        ByteBuffer.wrap("18:00:00.012340".getBytes()),
        ByteBuffer.wrap("08:00:00.123".getBytes()),
        ByteBuffer.wrap("08:00:00.123".getBytes()));
  }

  @Test
  void localTimeValuePrepare() {
    localTimeValue(sharedConnPrepare);
    validateNotNull(
        ByteBuffer.wrap((meta.isMariaDBServer() ? "18:00:00.012340" : "18:00:00").getBytes()),
        ByteBuffer.wrap((meta.isMariaDBServer() ? "08:00:00.123000" : "08:00:00").getBytes()),
        ByteBuffer.wrap((meta.isMariaDBServer() ? "08:00:00.123000" : "08:00:00").getBytes()));
  }

  private void localTimeValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, LocalTime.parse("18:00:00.012340"))
        .bind(1, LocalTime.parse("08:00:00.123"))
        .bind(2, LocalTime.parse("08:00:00.123"))
        .execute()
        .blockLast();
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

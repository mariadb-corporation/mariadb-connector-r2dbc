// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class ProcedureResultsetTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    dropAll();
    sharedConn
        .createStatement(
            "CREATE PROCEDURE basic_proc (IN t1 INT, INOUT t2 INT unsigned, OUT t3 INT, IN t4 INT, OUT t5 VARCHAR(20) CHARSET utf8mb4, OUT t6 TIMESTAMP, OUT t7 VARCHAR(20) CHARSET utf8mb4) BEGIN \n"
                + "SELECT 1;\n"
                + "set t3 = t1 * t4;\n"
                + "set t2 = t2 * t1;\n"
                + "set t5 = 'http://test';\n"
                + "set t6 = TIMESTAMP('2003-12-31 12:00:00');\n"
                + "set t7 = 'test';\n"
                + "END")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE PROCEDURE no_out_proc (IN t1 INT, IN t2 INT) BEGIN \n"
                + "DO t1 + t2;\n"
                + "END")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void dropAll() {
    sharedConn.createStatement("DROP PROCEDURE IF EXISTS basic_proc").execute().blockLast();
    sharedConn.createStatement("DROP PROCEDURE IF EXISTS no_out_proc").execute().blockLast();
  }

  @Test
  void outputParameter() {
    // https://jira.mariadb.org/browse/XPT-268
    Assumptions.assumeFalse(isXpand());
    List<List<Object>> l =
        sharedConn
            .createStatement("call basic_proc(?,?,?,?,?,?,?)")
            .bind(0, 2)
            .bind(1, Parameters.inOut(2))
            .bind(2, Parameters.out(R2dbcType.INTEGER))
            .bind(3, 10)
            .bind(4, Parameters.out(R2dbcType.VARCHAR))
            .bind(5, Parameters.out(R2dbcType.TIMESTAMP))
            .bind(6, Parameters.out(R2dbcType.VARCHAR))
            .execute()
            .flatMap(
                r ->
                    Flux.from(
                            r.filter(Result.OutSegment.class::isInstance)
                                .flatMap(
                                    seg -> {
                                      OutParametersMetadata metas =
                                          ((Result.OutSegment) seg).outParameters().getMetadata();

                                      assertThrows(
                                          IllegalArgumentException.class,
                                          () -> metas.getParameterMetadata(-1),
                                          "Column index -1 is not in permit range[0,4]");
                                      assertThrows(
                                          IllegalArgumentException.class,
                                          () -> metas.getParameterMetadata(10),
                                          "Column index 10 is not in permit range[0,4]");

                                      Assertions.assertEquals(
                                          metas.getParameterMetadata(0),
                                          metas.getParameterMetadata("t2"));
                                      Assertions.assertEquals(
                                          5, metas.getParameterMetadatas().size());
                                      assertThrows(
                                          NoSuchElementException.class,
                                          () -> metas.getParameterMetadata("wrong"),
                                          "Column name 'wrong' does not exist in column names [");
                                      return Flux.just(
                                          ((Result.OutSegment) seg).outParameters().get(0),
                                          ((Result.OutSegment) seg).outParameters().get(1),
                                          ((Result.OutSegment) seg).outParameters().get(2),
                                          ((Result.OutSegment) seg).outParameters().get(3),
                                          ((Result.OutSegment) seg).outParameters().get(4));
                                    }))
                        .collectList())
            .collectList()
            .block();

    Assertions.assertEquals(2, l.size());
    Assertions.assertEquals(0, l.get(0).size());
    Assertions.assertEquals(5, l.get(1).size());
    Assertions.assertEquals(4L, l.get(1).get(0));
    Assertions.assertEquals(20, l.get(1).get(1));
    if (isMariaDBServer() && minVersion(10, 3, 0)) {
      Assertions.assertEquals("http://test", l.get(1).get(2));
      Assertions.assertEquals("test", l.get(1).get(4));
    }
    Assertions.assertEquals(LocalDateTime.parse("2003-12-31T12:00:00"), l.get(1).get(3));

    List<List<Object>> l2 =
        sharedConn
            .createStatement("/*text*/ call basic_proc(?,?,?,?,?,?,?)")
            .bind(0, 2)
            .bind(1, Parameters.inOut(2))
            .bind(2, Parameters.out(R2dbcType.INTEGER))
            .bind(3, 10)
            .bind(4, Parameters.out(R2dbcType.VARCHAR))
            .bind(5, Parameters.out(R2dbcType.TIMESTAMP))
            .bind(6, Parameters.out(R2dbcType.VARCHAR))
            .execute()
            .flatMap(
                r ->
                    Flux.from(
                            r.filter(Result.OutSegment.class::isInstance)
                                .flatMap(
                                    seg -> {
                                      return Flux.just(
                                          ((Result.OutSegment) seg).outParameters().get(0),
                                          ((Result.OutSegment) seg).outParameters().get(1),
                                          ((Result.OutSegment) seg).outParameters().get(2),
                                          ((Result.OutSegment) seg).outParameters().get(3),
                                          ((Result.OutSegment) seg).outParameters().get(4));
                                    }))
                        .collectList())
            .collectList()
            .block();

    Assertions.assertEquals(1, l2.size());
  }

  @Test
  void inParameter() {
    Assumptions.assumeFalse(isXpand());
    sharedConn
        .createStatement("call no_out_proc(?,?)")
        .bind(0, 2)
        .bind(1, 10)
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(0L)
        .verifyComplete();
    sharedConn
        .createStatement("/*text*/ call no_out_proc(?,?)")
        .bind(0, 2)
        .bind(1, 10)
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(0L)
        .verifyComplete();
  }
}

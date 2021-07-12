// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbResult;
import reactor.core.publisher.Flux;

public class ProcedureResultsetTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    dropAll();
    sharedConn
        .createStatement(
            "CREATE PROCEDURE basic_proc (IN t1 INT, INOUT t2 INT unsigned, OUT t3 INT, IN t4 INT, OUT t5 VARCHAR(20), OUT t6 TIMESTAMP, OUT t7 VARCHAR(20)) BEGIN \n"
                + "SELECT 1;\n"
                + "set t3 = t1 * t4;\n"
                + "set t2 = t2 * t1;\n"
                + "set t5 = 'http://test';\n"
                + "set t6 = TIMESTAMP('2003-12-31 12:00:00');\n"
                + "set t7 = 'test';\n"
                + "END")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void dropAll() {
    sharedConn.createStatement("DROP PROCEDURE IF EXISTS basic_proc").execute().blockLast();
  }

  @Test
  void outputParameter() {
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
                    ((MariadbResult) r.filter(Result.OutSegment.class::isInstance))
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
                                  metas.getParameterMetadata(0), metas.getParameterMetadata("t2"));
                              Assertions.assertEquals(5, metas.getParameterMetadatas().size());
                              assertThrows(
                                  IllegalArgumentException.class,
                                  () -> metas.getParameterMetadata("wrong"),
                                  "Column name 'wrong' does not exist in column names [t2, t3, t5, t6, t7]");
                              return Flux.just(
                                  ((Result.OutSegment) seg).outParameters().get(0),
                                  ((Result.OutSegment) seg).outParameters().get(1),
                                  ((Result.OutSegment) seg).outParameters().get(2),
                                  ((Result.OutSegment) seg).outParameters().get(3),
                                  ((Result.OutSegment) seg).outParameters().get(4));
                            })
                        .collectList())
            .collectList()
            .block();

    Assertions.assertEquals(2, l.size());
    Assertions.assertEquals(0, l.get(0).size());
    Assertions.assertEquals(5, l.get(1).size());
    Assertions.assertEquals(4L, l.get(1).get(0));
    Assertions.assertEquals(20, l.get(1).get(1));
    Assertions.assertEquals("http://test", l.get(1).get(2));
    Assertions.assertEquals(LocalDateTime.parse("2003-12-31T12:00:00"), l.get(1).get(3));
    Assertions.assertEquals("test", l.get(1).get(4));
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.sql.ResultSet;
import java.sql.Statement;

public class Select_1_user extends Common {

  final int numberOfUserCol = 46;

  @Benchmark
  public Object[] testJdbc(MyState state) throws Throwable {
    return consumeJdbc(state.jdbc);
  }

  @Benchmark
  public Object[] testR2dbc(MyState state) throws Throwable {
    return consume(state.r2dbc);
  }

  @Benchmark
  public Object[] testJdbcPrepare(MyState state) throws Throwable {
    return consumeJdbc(state.jdbcPrepare);
  }

  @Benchmark
  public Object[] testR2dbcPrepare(MyState state) throws Throwable {
    return consume(state.r2dbcPrepare);
  }

//  @Benchmark
//  public Object[] testR2dbcMySql(MyState state) throws Throwable {
//    return consume(state.r2dbcMysql, blackhole);
//  }

  private Object[] consume(io.r2dbc.spi.Connection connection) {
    io.r2dbc.spi.Statement statement =
        connection.createStatement("select * FROM mysql.user WHERE 1 = ? LIMIT 1")
            .bind(0, 1);
    return
        Flux.from(statement.execute())
            .flatMap(
                it ->
                    it.map(
                        (row, rowMetadata) -> {
                          Object[] objs = new Object[numberOfUserCol];
                          for (int i = 0; i < numberOfUserCol; i++) {
                            objs[i] = row.get(i);
                          }
                          return objs;
                        }))
            .blockLast();
  }


  private Object[] consumeJdbc(java.sql.Connection connection) throws java.sql.SQLException {
    try (java.sql.PreparedStatement prep = connection.prepareStatement("select * FROM mysql.user WHERE 1 = ? LIMIT 1")) {
      prep.setInt(1, 1);
      ResultSet rs = prep.executeQuery();
      rs.next();
      Object[] objs = new Object[numberOfUserCol];
      for (int i = 0; i < numberOfUserCol; i++) {
        objs[i] = rs.getObject(i + 1);
      }
      return objs;
    }
  }
}

/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.sql.ResultSet;
import java.sql.Statement;

public class Select_1 extends Common {

  @Benchmark
  public int testJdbc(MyState state) throws Throwable {
    return consumeJdbc(state.jdbc);
  }

  @Benchmark
  public Integer testR2dbc(MyState state) throws Throwable {
    return consume(state.r2dbc);
  }

  @Benchmark
  public int testJdbcPrepare(MyState state) throws Throwable {
    return consumeJdbc(state.jdbcPrepare);
  }

  @Benchmark
  public Integer testR2dbcPrepare(MyState state) throws Throwable {
    return consume(state.r2dbcPrepare);
  }

//  @Benchmark
//  public void testR2dbcMysql(MyState state, Blackhole blackhole) throws Throwable {
//    consume(state.r2dbcMysql, blackhole);
//  }

  private Integer consume(io.r2dbc.spi.Connection connection) {
    int rnd = (int) (Math.random() * 1000);
    io.r2dbc.spi.Statement statement = connection.createStatement("select " + rnd);
    Integer val =
        Flux.from(statement.execute())
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .blockLast();
    if (rnd != val)
      throw new IllegalStateException("ERROR rnd:" + rnd + " different to val:" + val);
    return val;
  }

  private int consumeJdbc(java.sql.Connection connection) throws java.sql.SQLException {
    try (java.sql.PreparedStatement prep = connection.prepareStatement("select ?")) {
      int rnd = (int) (Math.random() * 1000);
      prep.setInt(1, rnd);
      ResultSet rs = prep.executeQuery();
      rs.next();
      int val = rs.getInt(1);
      if (rnd != val) throw new IllegalStateException("ERROR");
      return val;
    }
  }

}

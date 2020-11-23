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

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Select_1000_params extends Common {

  private static final String sql;

  static {
    StringBuilder sb = new StringBuilder("select ?");
    for (int i = 1; i < 1000; i++) {
      sb.append(",?");
    }
    sql = sb.toString();
  }

  private static int[] randParams() {
    int[] rnds = new int[1000];
    for (int i = 0; i < 1000; i++) {
      rnds[i] = (int) (Math.random() * 1000);
    }
    return rnds;
  }

  @Benchmark
  public void testJdbc(MyState state, Blackhole blackhole) throws Throwable {
    int[] rnds = randParams();
    PreparedStatement st = state.jdbc.prepareStatement(sql);
    for (int i = 1; i <= 1000; i++) {
      st.setInt(i, rnds[i - 1]);
    }
    ResultSet rs = st.executeQuery();
    rs.next();
    Integer val = rs.getInt(1);
    if (rnds[0] != val) throw new IllegalStateException("ERROR");
    blackhole.consume(val);
  }

  @Benchmark
  public void testR2dbc(MyState state, Blackhole blackhole) throws Throwable {
    consume(state.r2dbc, blackhole);
  }

  @Benchmark
  public void testR2dbcPrepare(MyState state, Blackhole blackhole) throws Throwable {
    consume(state.r2dbcPrepare, blackhole);
  }

//  @Benchmark
//  public void testR2dbcMysql(MyState state, Blackhole blackhole) throws Throwable {
//    consume(state.r2dbcMysql, blackhole);
//  }

  private void consume(io.r2dbc.spi.Connection connection, Blackhole blackhole) {
    int[] rnds = randParams();

    io.r2dbc.spi.Statement statement = connection.createStatement(sql);
    for (int i = 0; i < 1000; i++) {
      statement.bind(i, rnds[i]);
    }

    Integer val =
        Flux.from(statement.execute())
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .blockLast();
    if (rnds[0] != val) throw new IllegalStateException("ERROR");
    blackhole.consume(val);
  }
}

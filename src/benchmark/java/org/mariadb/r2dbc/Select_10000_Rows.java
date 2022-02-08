// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;

public class Select_10000_Rows extends Common {
  private static final String sql =
      "SELECT lpad(conv(floor(rand()*pow(36,8)), 10, 36), 8, 0) as rnd_str_8 FROM seq_1_to_10000 WHERE 1 = ?";

  @Benchmark
  public String[] testJdbc(MyState state, Blackhole blackhole) throws Throwable {
    try (PreparedStatement st = state.jdbc.prepareStatement(sql)) {
      st.setInt(1, 1);
      ResultSet rs = st.executeQuery();
      String[] res = new String[10000];
      int i = 0;
      while (rs.next()) {
        res[i++] = rs.getString(1);
      }
      return res;
    }
  }

  @Benchmark
  public List<String> testR2dbc(MyState state, Blackhole blackhole) throws Throwable {
    return consume(state.r2dbc, blackhole);
  }

  @Benchmark
  public List<String> testR2dbcPrepare(MyState state, Blackhole blackhole) throws Throwable {
    return consume(state.r2dbcPrepare, blackhole);
  }

//  @Benchmark
//  public void testR2dbcMysql(MyState state, Blackhole blackhole) throws Throwable {
//    consume(state.r2dbcMysql, blackhole);
//  }

  private List<String> consume(io.r2dbc.spi.Connection connection, Blackhole blackhole) {
    return Flux.from(connection.createStatement(sql).bind(0,1).execute())
        .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, String.class)))
            .collectList().block(Duration.ofSeconds(1));
  }
}

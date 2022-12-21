// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class Select_1000_Rows extends Common {
  private static final String sql =
          "select seq, 'abcdefghijabcdefghijabcdefghijaa' from seq_1_to_1000";

  @Benchmark
  public Integer testR2dbc(MyState state, Blackhole blackhole) throws Throwable {
    return consume(state.r2dbc, blackhole);
  }

  @Benchmark
  public Integer testR2dbcPrepare(MyState state, Blackhole blackhole) throws Throwable {
    return consumePrepare(state.r2dbcPrepare, blackhole);
  }

  private Integer consume(MariadbConnection connection, Blackhole blackhole) {
      return connection.createStatement(sql).execute()
        .flatMap(it -> it.map((row, rowMetadata) -> {
          Integer i = row.get(0, Integer.class);
          row.get(1, String.class);
          return i;
        })).blockLast();
  }

    private Integer consumePrepare(MariadbConnection connection, Blackhole blackhole) {
        return connection.createStatement(sql + " WHERE 1 = ?").bind(0,1).execute()
                .flatMap(it -> it.map((row, rowMetadata) -> {
                    Integer i = row.get(0, Integer.class);
                    row.get(1, String.class);
                    return i;
                })).blockLast();
    }

}

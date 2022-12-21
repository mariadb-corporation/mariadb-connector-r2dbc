// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import org.mariadb.r2dbc.api.MariadbStatement;
import org.openjdk.jmh.annotations.Benchmark;

public class Select_100_cols extends Common {

  @Benchmark
  public int[] testR2dbc(MyState state) throws Throwable {
    return consume(state.r2dbc);
  }

  @Benchmark
  public int[] testR2dbcPrepare(MyState state) throws Throwable {
    return consumePrepare(state.r2dbcPrepare);
  }

  private int[] consume(MariadbConnection connection) {

    MariadbStatement statement =
        connection.createStatement("select * FROM test100");
    return
        statement.execute()
            .flatMap(
                it ->
                    it.map(
                        (row, rowMetadata) -> {
                          int[] objs = new int[100];
                          for (int i = 0; i < 100; i++) {
                            objs[i] = row.get(i, Integer.class);
                          }
                          return objs;
                        }))
            .blockLast();
  }

    private int[] consumePrepare(MariadbConnection connection) {

        MariadbStatement statement =
                connection.createStatement("select * FROM test100 WHERE 1 = ?").bind(0,1);
        return
                statement.execute()
                        .flatMap(
                                it ->
                                        it.map(
                                                (row, rowMetadata) -> {
                                                    int[] objs = new int[100];
                                                    for (int i = 0; i < 100; i++) {
                                                        objs[i] = row.get(i, Integer.class);
                                                    }
                                                    return objs;
                                                }))
                        .blockLast();
    }
}

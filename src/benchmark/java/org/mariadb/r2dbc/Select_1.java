// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import org.mariadb.r2dbc.api.MariadbStatement;
import org.openjdk.jmh.annotations.Benchmark;

public class Select_1 extends Common {

  @Benchmark
  public Integer testR2dbc(MyState state) throws Throwable {
    return consume(state.r2dbc);
  }

  @Benchmark
  public Integer testR2dbcPrepare(MyState state) throws Throwable {
    return consume(state.r2dbcPrepare);
  }

  private Integer consume(MariadbConnection connection) {
    int rnd = (int) (Math.random() * 1000);
    MariadbStatement statement = connection.createStatement("select " + rnd);
    return
        statement.execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .blockLast();
  }

}

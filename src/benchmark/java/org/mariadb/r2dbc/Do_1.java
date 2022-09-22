// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import org.openjdk.jmh.annotations.Benchmark;
import reactor.core.publisher.Flux;

public class Do_1 extends Common {

  @Benchmark
  public Integer testR2dbc(MyState state) throws Throwable {
    return consume(state.r2dbc);
  }

  @Benchmark
  public Integer testR2dbcPrepare(MyState state) throws Throwable {
    return consume(state.r2dbcPrepare);
  }

  private Integer consume(io.r2dbc.spi.Connection connection) {
    io.r2dbc.spi.Statement statement = connection.createStatement("DO 1");
    return
        Flux.from(statement.execute())
            .flatMap(it -> it.getRowsUpdated())
            .blockLast();
  }


}

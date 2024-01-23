// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import org.openjdk.jmh.annotations.Benchmark;

public class Do_1 extends Common {

    @Benchmark
    public Long testR2dbc(MyState state) throws Throwable {
        return consume(state.r2dbc);
    }

    @Benchmark
    public Long testR2dbcPrepare(MyState state) throws Throwable {
        return consume(state.r2dbcPrepare);
    }

    private Long consume(MariadbConnection connection) {
        return connection.createStatement("DO 1").execute()
                .flatMap(it -> it.getRowsUpdated())
                .blockLast();
    }


}

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

public class Select_1_user extends Common {

  final int numberOfUserCol = 46;

  @Benchmark
  public void testJdbc(MyState state, Blackhole blackhole) throws Throwable {
    Statement st = state.jdbc.createStatement();
    ResultSet rs = st.executeQuery("select * FROM mysql.user LIMIT 1");
    rs.next();
    Object[] objs = new Object[numberOfUserCol];
    for (int i = 0; i < numberOfUserCol; i++) {
      objs[i] = rs.getObject(i + 1);
    }
    blackhole.consume(objs);
  }

  @Benchmark
  public void testR2dbc(MyState state, Blackhole blackhole) throws Throwable {
    consume(state.r2dbc, blackhole);
  }

  @Benchmark
  public void testR2dbcPrepare(MyState state, Blackhole blackhole) throws Throwable {
    consume(state.r2dbcPrepare, blackhole);
  }

  @Benchmark
  public void testR2dbcMySql(MyState state, Blackhole blackhole) throws Throwable {
    consume(state.r2dbcMysql, blackhole);
  }

  private void consume(io.r2dbc.spi.Connection connection, Blackhole blackhole) {
    io.r2dbc.spi.Statement statement =
        connection.createStatement("select * FROM mysql.user LIMIT 1");
    Object[] obj =
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
    blackhole.consume(obj);
  }
}

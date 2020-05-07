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

import dev.miku.r2dbc.mysql.MySqlConnectionConfiguration;
import dev.miku.r2dbc.mysql.MySqlConnectionFactory;
import dev.miku.r2dbc.mysql.constant.SslMode;
import org.openjdk.jmh.annotations.*;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 1)
@Threads(value = -1) // detecting CPU count
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class Common {

  @State(Scope.Thread)
  public static class MyState {

    // conf
    public final String host = System.getProperty("TEST_HOST", "localhost");
    public final int port = Integer.parseInt(System.getProperty("TEST_PORT", "3306"));
    public final String username = System.getProperty("TEST_USERNAME", "root");
    public final String password = System.getProperty("TEST_PASSWORD", "");
    public final String database = System.getProperty("TEST_DATABASE", "testj");

    // connections
    protected Connection jdbc;
    protected io.r2dbc.spi.Connection r2dbc;
    protected io.r2dbc.spi.Connection r2dbcPrepare;
    protected io.r2dbc.spi.Connection r2dbcMysql;

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
      MariadbConnectionConfiguration conf =
          MariadbConnectionConfiguration.builder()
              .host(host)
              .port(port)
              .username(username)
              .password(password)
              .database(database)
              .build();

      MariadbConnectionConfiguration confPrepare =
          MariadbConnectionConfiguration.builder()
              .host(host)
              .port(port)
              .username(username)
              .password(password)
              .database(database)
              .useServerPrepStmts(true)
              .build();

      MySqlConnectionConfiguration confMysql =
          MySqlConnectionConfiguration.builder()
              .host(host)
              .username(username)
              .database(database)
              .password(password)
              .sslMode(SslMode.DISABLED)
              .port(port)
              .build();
      String jdbcUrl =
          String.format(
              "mariadb://%s:%s/%s?user=%s&password=%s", host, port, database, username, password);

      try {
        jdbc = DriverManager.getConnection("jdbc:" + jdbcUrl);
        r2dbc = MariadbConnectionFactory.from(conf).create().block();
        r2dbcPrepare = MariadbConnectionFactory.from(confPrepare).create().block();
        r2dbcMysql = MySqlConnectionFactory.from(confMysql).create().block();

      } catch (SQLException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws SQLException {
      jdbc.close();
      Mono.from(r2dbc.close()).block();
      Mono.from(r2dbcPrepare.close()).block();
      Mono.from(r2dbcMysql.close()).block();
    }
  }
}

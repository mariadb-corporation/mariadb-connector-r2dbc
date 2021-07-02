// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import org.openjdk.jmh.annotations.*;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
@Threads(value = -1) // detecting CPU count
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Timeout(timeUnit = TimeUnit.SECONDS, time = 2)
public class Common {

  @State(Scope.Thread)
  public static class MyState {

    // conf
    public final String host = System.getProperty("TEST_HOST", "localhost");
    public final int port = Integer.parseInt(System.getProperty("TEST_PORT", "3306"));
    public final String username = System.getProperty("TEST_USERNAME", "root");
    public final String password = System.getProperty("TEST_PASSWORD", "");
    public final String database = System.getProperty("TEST_DATABASE", "testr2");

    // connections
    protected Connection jdbc;
    protected Connection jdbcPrepare;

    protected io.r2dbc.spi.Connection r2dbc;
    protected io.r2dbc.spi.Connection r2dbcPrepare;
//    protected io.r2dbc.spi.Connection r2dbcMysql;

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

//      MySqlConnectionConfiguration confMysql =
//          MySqlConnectionConfiguration.builder()
//              .host(host)
//              .username(username)
//              .database(database)
//              .password(password)
//              .sslMode(SslMode.DISABLED)
//              .port(port)
//              .build();
      String jdbcUrl =
          String.format(
              "mariadb://%s:%s/%s?user=%s&password=%s", host, port, database, username, password);

      try {
        jdbc = DriverManager.getConnection("jdbc:" + jdbcUrl);
        jdbcPrepare = DriverManager.getConnection("jdbc:" + jdbcUrl + "&useServerPrepStmts=true");
        r2dbc = MariadbConnectionFactory.from(conf).create().block();
        r2dbcPrepare = MariadbConnectionFactory.from(confPrepare).create().block();
//        r2dbcMysql = MySqlConnectionFactory.from(confMysql).create().block();

      } catch (SQLException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws SQLException {
      jdbc.close();
      jdbcPrepare.close();
      Mono.from(r2dbc.close()).block();
      Mono.from(r2dbcPrepare.close()).block();
//      Mono.from(r2dbcMysql.close()).block();
    }
  }
}

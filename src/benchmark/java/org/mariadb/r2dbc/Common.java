// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.ConnectionFactories;
import org.openjdk.jmh.annotations.*;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 5)
@Threads(value = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Timeout(timeUnit = TimeUnit.SECONDS, time = 2)
public class Common {

  // conf
  public static final String host = System.getProperty("TEST_HOST", "localhost");
  public static final int port = Integer.parseInt(System.getProperty("TEST_PORT", "3306"));
  public static final String username = System.getProperty("TEST_USERNAME", "root");
  public static final String password = System.getProperty("TEST_PASSWORD", "");
  public static final String database = System.getProperty("TEST_DATABASE", "testr2");
  static {
    new SetupData();
  }

  @State(Scope.Thread)
  public static class MyState {

    protected MariadbConnection r2dbc;
    protected MariadbConnection r2dbcPrepare;

    @Setup(Level.Trial)
    @SuppressWarnings("unchecked")
    public void doSetup() throws Exception {
      String url =
          String.format(
              "r2dbc:mariadb://%s:%s@%s:%s/%s", username, password, host, port, database);

      String urlPrepare =
              String.format(
                      "r2dbc:mariadb://%s:%s@%s:%s/%s?useServerPrepStmts=true", username, password, host, port, database);

      r2dbc = ((Mono<org.mariadb.r2dbc.MariadbConnection>)ConnectionFactories.get(url).create()).block();
      r2dbcPrepare = ((Mono<org.mariadb.r2dbc.MariadbConnection>)ConnectionFactories.get(urlPrepare).create()).block();
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws SQLException {
      Mono.from(r2dbc.close()).block();
      Mono.from(r2dbcPrepare.close()).block();
    }
  }


  public static class SetupData {
    static {
      try {
        try (Connection conn =
                     DriverManager.getConnection(
                             String.format(
                                     "jdbc:mariadb://%s:%s/%s?user=%s&password=%s",
                                     host, port, database, username, password))) {
          Statement stmt = conn.createStatement();
          try {
            stmt.executeQuery("INSTALL SONAME 'ha_blackhole'");
          } catch (SQLException e) {
            // eat
          }
          stmt.executeUpdate("DROP TABLE IF EXISTS testBlackHole");
          stmt.executeUpdate("DROP TABLE IF EXISTS test100");

          try {
            stmt.executeUpdate("CREATE TABLE testBlackHole (id INT, t VARCHAR(256)) ENGINE = BLACKHOLE");
          } catch (SQLException e) {
            stmt.executeUpdate("CREATE TABLE testBlackHole (id INT, t VARCHAR(256))");
          }

          StringBuilder sb = new StringBuilder("CREATE TABLE test100 (i1 int");
          StringBuilder sb2 = new StringBuilder("INSERT INTO test100 value (1");
          for (int i = 2; i <= 100; i++) {
            sb.append(",i").append(i).append(" int");
            sb2.append(",").append(i);
          }
          sb.append(")");
          sb2.append(")");
          stmt.executeUpdate(sb.toString());
          stmt.executeUpdate(sb2.toString());

          stmt.execute("DROP TABLE IF EXISTS perfTestTextBatch");
          try {
            stmt.execute("INSTALL SONAME 'ha_blackhole'");
          } catch (SQLException e) { }

          String createTable = "CREATE TABLE perfTestTextBatch (id MEDIUMINT NOT NULL AUTO_INCREMENT,t0 text, PRIMARY KEY (id)) COLLATE='utf8mb4_unicode_ci'";
          try {
            stmt.execute(createTable + " ENGINE = BLACKHOLE");
          } catch (SQLException e){
            stmt.execute(createTable);
          }

        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}

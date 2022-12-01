// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.ValidationDepth;
import java.io.IOException;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.function.Executable;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import org.mariadb.r2dbc.tools.TcpProxy;
import org.mariadb.r2dbc.util.HostAddress;
import reactor.test.StepVerifier;

public class BaseConnectionTest extends BaseTest {
  public static MariadbConnectionFactory factory = TestConfiguration.defaultFactory;
  public static MariadbConnection sharedConn;
  public static MariadbConnection sharedConnPrepare;
  public static int initialConnectionNumber = -1;
  public static TcpProxy proxy;
  private static final Random rand = new Random();
  public static final Boolean backslashEscape =
      System.getenv("NO_BACKSLASH_ESCAPES") != null
          ? Boolean.valueOf(System.getenv("NO_BACKSLASH_ESCAPES"))
          : false;

  @BeforeAll
  public static void beforeAll() throws Exception {

    sharedConn = factory.create().block();
    MariadbConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder.clone().useServerPrepStmts(true).build();
    sharedConnPrepare = new MariadbConnectionFactory(confPipeline).create().block();
    String sqlModeAddition = "";
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    if ((meta.isMariaDBServer() && !meta.minVersion(10, 2, 4)) || !meta.isMariaDBServer()) {
      sqlModeAddition += ",STRICT_TRANS_TABLES";
    }
    if ((meta.isMariaDBServer() && !meta.minVersion(10, 1, 7)) || !meta.isMariaDBServer()) {
      sqlModeAddition += ",NO_ENGINE_SUBSTITUTION";
    }
    if (backslashEscape) {
      sqlModeAddition += ",NO_BACKSLASH_ESCAPES";
    }
    if (!"".equals(sqlModeAddition)) {
      sharedConn
          .createStatement("SET @@sql_mode = concat(@@sql_mode,'" + sqlModeAddition + "')")
          .execute()
          .blockLast();
      sharedConnPrepare
          .createStatement("set sql_mode= concat(@@sql_mode,'" + sqlModeAddition + "')")
          .execute()
          .blockLast();
    }
    initialConnectionNumber =
        sharedConn
            .createStatement("SHOW STATUS WHERE `variable_name` = 'Threads_connected'")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(1, Integer.class)))
            .blockLast();
  }

  public MariadbConnection createProxyCon() throws Exception {
    HostAddress hostAddress = TestConfiguration.defaultConf.getHostAddresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.getHost(), hostAddress.getPort());
    } catch (IOException i) {
      throw new Exception("proxy error", i);
    }
    MariadbConnectionConfiguration confProxy =
        TestConfiguration.defaultBuilder
            .clone()
            .port(proxy.getLocalPort())
            .host(System.getenv("TRAVIS") != null ? hostAddress.getHost() : "localhost")
            .build();
    return new MariadbConnectionFactory(confProxy).create().block();
  }

  @AfterEach
  public void afterEach1() {
    int finalConnectionNumber =
        sharedConn
            .createStatement("SHOW STATUS WHERE `variable_name` = 'Threads_connected'")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(1, Integer.class)))
            .blockLast();
    if (finalConnectionNumber - initialConnectionNumber > 0) {
      System.err.println(
          String.format(
              "Error connection not ended : changed=%s (ended:%s initial:%s)",
              (finalConnectionNumber - initialConnectionNumber),
              initialConnectionNumber,
              finalConnectionNumber));
    }
    initialConnectionNumber = finalConnectionNumber;

    int j = rand.nextInt();
    sharedConnPrepare
        .createStatement("SELECT " + j + ", 'b'")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(j)
        .verifyComplete();
  }

  @AfterAll
  public static void afterEAll() {
    sharedConn.close().block();
    sharedConnPrepare.close().block();
  }

  public static boolean runLongTest() {
    String runLongTest = System.getenv("RUN_LONG_TEST");
    if (runLongTest != null) {
      return Boolean.parseBoolean(runLongTest);
    }
    return false;
  }

  public static void assertThrowsContains(
      Class<? extends Exception> expectedType, Executable executable, String expected) {
    Exception e = Assertions.assertThrows(expectedType, executable);
    Assertions.assertTrue(e.getMessage().contains(expected), "real message:" + e.getMessage());
  }

  public static boolean isMariaDBServer() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    return meta.isMariaDBServer();
  }

  public static boolean minVersion(int major, int minor, int patch) {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    return meta.minVersion(major, minor, patch);
  }

  public static Integer maxAllowedPacket() {
    return sharedConnPrepare
        .createStatement("select @@max_allowed_packet")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .single()
        .block();
  }

  @AfterEach
  public void afterPreEach() {
    sharedConn
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
        .verifyComplete();
  }

  public boolean haveSsl(MariadbConnection connection) {
    return connection
        .createStatement("select @@have_ssl")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .blockLast()
        .equals("YES");
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.function.Executable;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import org.mariadb.r2dbc.tools.TcpProxy;
import org.mariadb.r2dbc.util.HostAddress;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class BaseConnectionTest {
  public static final boolean isWindows =
      System.getProperty("os.name").toLowerCase().contains("win");
  public static final Boolean backslashEscape =
      System.getenv("NO_BACKSLASH_ESCAPES") != null
          ? Boolean.valueOf(System.getenv("NO_BACKSLASH_ESCAPES"))
          : false;
  private static final Random rand = new Random();
  public static MariadbConnectionFactory factory = TestConfiguration.defaultFactory;
  public static MariadbConnection sharedConn;
  public static MariadbConnection sharedConnPrepare;
  public static Integer initialConnectionNumber = -1;
  public static TcpProxy proxy;
  private static Instant initialTest;
  private static String maxscaleVersion = null;

  @RegisterExtension public Extension watcher = new BaseConnectionTest.Follow();

  @BeforeAll
  public static void beforeAll() throws Exception {

    sharedConn = factory.create().block();

    MariadbConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder.clone().useServerPrepStmts(true).build();
    sharedConnPrepare = new MariadbConnectionFactory(confPipeline).create().block();
    String sqlModeAddition = "";
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    if ((meta.isMariaDBServer() && !meta.minVersion(10, 2, 4)) || !meta.isMariaDBServer()) {
      sqlModeAddition += ",STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO";
    }
    if ((meta.isMariaDBServer() && !meta.minVersion(10, 1, 7)) || !meta.isMariaDBServer()) {
      sqlModeAddition += ",NO_ENGINE_SUBSTITUTION";
    }
    if ((meta.isMariaDBServer() && !meta.minVersion(10, 1, 7))) {
      sqlModeAddition += "NO_AUTO_CREATE_USER";
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

  public static boolean isXpand() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    return meta.getDatabaseVersion().toLowerCase().contains("xpand");
  }

  public static boolean minVersion(int major, int minor, int patch) {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    return meta.minVersion(major, minor, patch);
  }

  public static boolean isMaxscale() {
    if (maxscaleVersion == null) {
      return "maxscale".equals(System.getenv("srv")) || "maxscale".equals(System.getenv("DB_TYPE"));
    }
    return true;
  }

  public static String getHostSuffix() {
    return "@'%'";
  }

  public static boolean isEnterprise() {
    return "enterprise".equals(System.getenv("DB_TYPE"));
  }

  public static boolean exactVersion(int major, int minor, int patch) {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    return meta.getMajorVersion() == major
        && meta.getMinorVersion() == minor
        && meta.getPatchVersion() == patch;
  }

  public static Integer maxAllowedPacket() {
    return sharedConnPrepare
        .createStatement("select @@max_allowed_packet")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .single()
        .block();
  }

  public void assertThrows(
      Class<? extends Exception> expectedType, Executable executable, String expected) {
    Exception e = Assertions.assertThrows(expectedType, executable);
    Assertions.assertTrue(e.getMessage().contains(expected), "real message:" + e.getMessage());
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
            .host("localhost")
            .build();
    return new MariadbConnectionFactory(confProxy).create().block();
  }

  public boolean haveSsl(MariadbConnection connection) {
    if (!isMariaDBServer() && minVersion(8, 4, 0)) return true;
    return connection
        .createStatement("select @@have_ssl")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .blockLast()
        .equals("YES");
  }

  private class Follow implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
      if (!isXpand()) {
        initialConnectionNumber =
            sharedConn
                .createStatement("SHOW STATUS LIKE 'Threads_connected'")
                .execute()
                .flatMap(r -> r.map((row, metadata) -> row.get(1, Integer.class)))
                .blockLast();
      }
      initialTest = Instant.now();
      System.out.println(
          "       test : " + extensionContext.getTestMethod().get().getName() + " begin");
    }

    @AfterEach
    public void afterEach(ExtensionContext extensionContext) throws Exception {
      if (!isXpand()) {
        Integer finalConnectionNumber =
            sharedConn
                .createStatement("SHOW STATUS LIKE 'Threads_connected'")
                .execute()
                .flatMap(r -> r.map((row, metadata) -> row.get(1, Integer.class)))
                .onErrorResume(
                    t -> {
                      t.printStackTrace();
                      return Mono.just(-999999);
                    })
                .blockLast();
        if (finalConnectionNumber != null && finalConnectionNumber - initialConnectionNumber != 0) {
          int retry = 5;
          do {
            Thread.sleep(50);
            finalConnectionNumber =
                sharedConn
                    .createStatement("SHOW STATUS LIKE 'Threads_connected'")
                    .execute()
                    .flatMap(r -> r.map((row, metadata) -> row.get(1, Integer.class)))
                    .onErrorResume(
                        t -> {
                          t.printStackTrace();
                          return Mono.just(-999999);
                        })
                    .blockLast();
          } while (retry-- > 0 && finalConnectionNumber != initialConnectionNumber);

          if (finalConnectionNumber - initialConnectionNumber != 0) {
            System.err.printf(
                "%s: Error connection not ended : changed=%s (initial:%s ended:%s)%n",
                extensionContext.getTestMethod().get(),
                (finalConnectionNumber - initialConnectionNumber),
                initialConnectionNumber,
                finalConnectionNumber);
          }
        }
      }
      System.out.println(
          "       test : "
              + extensionContext.getTestMethod().get().getName()
              + " "
              + Duration.between(initialTest, Instant.now()).toString());

      int j = rand.nextInt();
      sharedConnPrepare
          .createStatement("SELECT " + j + ", 'b'")
          .execute()
          .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
          .as(StepVerifier::create)
          .expectNext(j)
          .verifyComplete();
    }
  }
}

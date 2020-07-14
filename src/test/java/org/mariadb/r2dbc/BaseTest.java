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

import io.r2dbc.spi.ValidationDepth;
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
import reactor.test.StepVerifier;

public class BaseTest {
  public static MariadbConnectionFactory factory = TestConfiguration.defaultFactory;
  public static MariadbConnection sharedConn;
  public static MariadbConnection sharedConnPrepare;
  private static Random rand = new Random();
  public static final Boolean backslashEscape =
      Boolean.valueOf(System.getProperty("NO_BACKSLASH_ESCAPES", "false"));

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
          .createStatement("SET @@sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')")
          .execute()
          .blockLast();
      sharedConnPrepare
          .createStatement("set sql_mode= concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')")
          .execute()
          .blockLast();
    }

    System.out.println(sharedConn
        .createStatement("SELECT @@sql_mode")
        .execute()
        .flatMap(r -> r.map((row, rowMetadata) -> row.get(0, String.class)))
        .single()
        .block());

  }

  @AfterEach
  public void afterEach1() {
    int i = rand.nextInt();
    sharedConn
        .createStatement("SELECT " + i)
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(i)
        .verifyComplete();

    int j = rand.nextInt();
    sharedConnPrepare
        .createStatement("SELECT " + j)
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

  //  @RegisterExtension public Extension watcher = new Follow();

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

  public void assertThrows(
      Class<? extends Exception> expectedType, Executable executable, String expected) {
    Exception e = Assertions.assertThrows(expectedType, executable);
    Assertions.assertTrue(e.getMessage().contains(expected), "real message:" + e.getMessage());
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

  private static Instant initialTest;

  private class Follow implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
      System.out.println(Duration.between(initialTest, Instant.now()).toString());
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
      initialTest = Instant.now();
      System.out.print("       test : " + extensionContext.getTestMethod().get() + " ");
    }
  }
}

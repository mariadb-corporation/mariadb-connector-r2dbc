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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.function.Executable;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.test.StepVerifier;

public class BaseTest {
  public static MariadbConnectionFactory factory = TestConfiguration.defaultFactory;
  public static MariadbConnection sharedConn;
  public static MariadbConnection sharedConnPrepare;

  @BeforeAll
  public static void beforeAll() throws Exception {
    sharedConn = factory.create().block();
    MariadbConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder.clone().useServerPrepStmts(true).build();
    sharedConnPrepare = new MariadbConnectionFactory(confPipeline).create().block();
  }

  @AfterEach
  public void after1() throws Throwable {
    System.gc();
  }

  @AfterEach
  public void afterEach1() {
    sharedConn
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
        .verifyComplete();
    sharedConnPrepare
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
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

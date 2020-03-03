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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.test.StepVerifier;

public class BaseTest {
  public static MariadbConnectionFactory factory = TestConfiguration.defaultFactory;
  public static MariadbConnection sharedConn;

  @BeforeAll
  public static void beforeAll() {
    sharedConn = factory.create().block();
  }

  @AfterAll
  public static void afterAll() {
    sharedConn
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
        .verifyComplete();
    sharedConn.close().block();
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

  private class Follow implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
      System.out.println("after test : " + extensionContext.getTestMethod().get());
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
      System.out.println("before test : " + extensionContext.getTestMethod().get());
    }
  }
}

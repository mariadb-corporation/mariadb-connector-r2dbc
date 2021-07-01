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

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;

public class ConnectionMetadataTest extends BaseConnectionTest {

  @Test
  void connectionMeta() {
    ConnectionMetadata meta = sharedConn.getMetadata();
    assertTrue(meta.getDatabaseProductName().equals(isMariaDBServer() ? "MariaDB" : "MySQL"));
    if (isMariaDBServer()) {
      assertTrue(meta.getDatabaseVersion().contains("10."));
    } else {
      assertTrue(
          meta.getDatabaseVersion().contains("5.") || meta.getDatabaseVersion().contains("8."));
    }
    String type = System.getenv("srv");
    String version = System.getenv("v");
    if (type != null && System.getenv("TRAVIS") != null) {
      if ("mariadb".equals(type) || "mysql".equals(type)) {
        assertTrue(meta.getDatabaseVersion().contains(version));
        assertEquals(type.toLowerCase(), meta.getDatabaseProductName().toLowerCase());
      }
    }
  }

  @Test
  void factoryMeta() {
    ConnectionFactoryMetadata meta = factory.getMetadata();
    assertEquals("MariaDB", meta.getName());
  }

  @Test
  void metadataInfo() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    assertTrue(meta.getMajorVersion() >= 5);
    assertTrue(meta.getMinorVersion() > -1);
    assertTrue(meta.getPatchVersion() > -1);
  }
}

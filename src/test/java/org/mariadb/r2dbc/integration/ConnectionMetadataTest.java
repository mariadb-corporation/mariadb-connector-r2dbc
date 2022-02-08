// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

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
        assertTrue(
            meta.getDatabaseVersion().contains(version),
            "Error " + meta.getDatabaseVersion() + " doesn't contains " + version);
        assertEquals(
            type.toLowerCase(),
            meta.getDatabaseProductName().toLowerCase(),
            "Error comparing " + type + " with " + meta.getDatabaseProductName());
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

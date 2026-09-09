// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;

public class ConnectionMetadataTest extends BaseConnectionTest {

  @Test
  void connectionMeta() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    String dbVersion = meta.getDatabaseVersion();
    assertEquals(isMariaDBServer() ? "MariaDB" : "MySQL", meta.getDatabaseProductName());
    // the reported version must be the one parsed from the handshake, without any "5.5.5-" prefix
    assertTrue(
        dbVersion.startsWith(
            meta.getMajorVersion() + "." + meta.getMinorVersion() + "." + meta.getPatchVersion()),
        "unexpected version " + dbVersion);
    if (isMariaDBServer() && !isXpand()) {
      assertTrue(meta.getMajorVersion() >= 10, "unexpected MariaDB version " + dbVersion);
    } else if (!isMariaDBServer()) {
      assertTrue(meta.getMajorVersion() >= 8, "unexpected MySQL version " + dbVersion);
    }
    String type = System.getenv("srv");
    String version = System.getenv("v");
    if (type != null && version != null && System.getenv("TRAVIS") != null) {
      if (version.endsWith("-rc")) version = version.replace("-rc", "");
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

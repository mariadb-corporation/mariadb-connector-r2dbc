// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.client.ServerVersion;

public class ServerVersionTest {
  @Test
  void testMinVersion() {
    ServerVersion sv = new ServerVersion("10.2.25-mariadb", true);
    assertEquals(10, sv.getMajorVersion());
    assertEquals(2, sv.getMinorVersion());
    assertEquals(25, sv.getPatchVersion());
    assertTrue(sv.versionGreaterOrEqual(9, 8, 8));
    assertTrue(sv.versionGreaterOrEqual(10, 1, 8));
    assertTrue(sv.versionGreaterOrEqual(10, 2, 8));
    assertTrue(sv.versionGreaterOrEqual(10, 2, 25));
    assertFalse(sv.versionGreaterOrEqual(19, 8, 8));
    assertFalse(sv.versionGreaterOrEqual(10, 3, 8));
    assertFalse(sv.versionGreaterOrEqual(10, 2, 30));

    ServerVersion sv2 = new ServerVersion("10.2.25", true);
    assertEquals(10, sv2.getMajorVersion());
    assertEquals(2, sv2.getMinorVersion());
    assertEquals(25, sv2.getPatchVersion());
  }
}

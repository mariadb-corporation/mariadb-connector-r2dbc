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

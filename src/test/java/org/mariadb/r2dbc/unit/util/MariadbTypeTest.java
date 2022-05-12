// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.util.MariadbType;

public class MariadbTypeTest {

  @Test
  void getName() {
    Assertions.assertEquals("VARCHAR", MariadbType.VARCHAR.getName());
    Assertions.assertEquals("BIGINT", MariadbType.UNSIGNED_BIGINT.getName());
    Assertions.assertEquals("UNSIGNED_BIGINT", MariadbType.UNSIGNED_BIGINT.name());
  }
}

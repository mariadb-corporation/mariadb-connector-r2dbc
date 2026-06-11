// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;

/**
 * {@link MariadbConnectionConfiguration#toString()} must not leak credentials. The primary password
 * is masked; the secondary PAM passwords (pamOtherPwd) must be masked the same way.
 */
public class ConfigurationToStringTest {

  @Test
  void passwordsAreMasked() {
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder()
            .host("localhost")
            .username("u")
            .password("primarySecret")
            .pamOtherPwd(new CharSequence[] {"PAM_SECRET_ONE", "PAM_SECRET_TWO"})
            .build();
    String s = conf.toString();
    assertFalse(s.contains("primarySecret"), s);
    assertFalse(s.contains("PAM_SECRET_ONE"), s);
    assertFalse(s.contains("PAM_SECRET_TWO"), s);
    assertTrue(s.contains("password=***"), s);
    assertTrue(s.contains("pamOtherPwd=***"), s);
  }
}

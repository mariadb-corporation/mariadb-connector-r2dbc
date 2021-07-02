// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.SslMode;

public class SslModeTest {

  @Test
  public void parse() throws Exception {
    Assertions.assertEquals(SslMode.DISABLE, SslMode.from("disable"));
    Assertions.assertEquals(SslMode.DISABLE, SslMode.from("DISABLE"));
    Assertions.assertEquals(SslMode.DISABLE, SslMode.from("DISABLED"));
    Assertions.assertEquals(SslMode.DISABLE, SslMode.from("0"));
    Assertions.assertEquals(SslMode.DISABLE, SslMode.from("false"));

    Assertions.assertThrows(IllegalArgumentException.class, () -> SslMode.from("wrong"));

    Assertions.assertEquals(SslMode.TRUST, SslMode.from("trust"));
    Assertions.assertEquals(SslMode.TRUST, SslMode.from("REQUIRED"));
    Assertions.assertEquals(SslMode.TRUST, SslMode.from("enable_trust"));

    Assertions.assertEquals(SslMode.VERIFY_CA, SslMode.from("verify-ca"));
    Assertions.assertEquals(SslMode.VERIFY_CA, SslMode.from("VERIFY_CA"));
    Assertions.assertEquals(
        SslMode.VERIFY_CA, SslMode.from("ENABLE_WITHOUT_HOSTNAME_VERIFICATION"));

    Assertions.assertEquals(SslMode.VERIFY_FULL, SslMode.from("verify-full"));
    Assertions.assertEquals(SslMode.VERIFY_FULL, SslMode.from("VERIFY_FULL"));
    Assertions.assertEquals(SslMode.VERIFY_FULL, SslMode.from("VERIFY_IDENTITY"));
    Assertions.assertEquals(SslMode.VERIFY_FULL, SslMode.from("1"));
    Assertions.assertEquals(SslMode.VERIFY_FULL, SslMode.from("true"));
    Assertions.assertEquals(SslMode.VERIFY_FULL, SslMode.from("enable"));
  }
}

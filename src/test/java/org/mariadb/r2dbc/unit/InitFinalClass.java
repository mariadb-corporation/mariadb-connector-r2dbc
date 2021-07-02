// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit;

import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.PidFactory;

public class InitFinalClass {

  @Test
  public void init() throws Exception {
    Codecs codecs = new Codecs();
    BufferUtils buf = new BufferUtils();
    PidFactory pid = new PidFactory();
    System.out.println(codecs.hashCode() + buf.hashCode() + pid.hashCode());
  }
}

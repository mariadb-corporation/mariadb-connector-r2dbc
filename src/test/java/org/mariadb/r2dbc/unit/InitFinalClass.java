// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit;

import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.constants.Capabilities;
import org.mariadb.r2dbc.util.constants.ColumnFlags;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import org.mariadb.r2dbc.util.constants.StateChange;

public class InitFinalClass {

  @Test
  public void init() throws Exception {
    Codecs codecs = new Codecs();
    BufferUtils buf = new BufferUtils();
    Capabilities c = new Capabilities();
    ColumnFlags c2 = new ColumnFlags();
    ServerStatus c3 = new ServerStatus();
    StateChange c4 = new StateChange();
    System.out.println(codecs.hashCode() + buf.hashCode());
  }
}

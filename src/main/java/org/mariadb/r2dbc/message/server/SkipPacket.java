// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import org.mariadb.r2dbc.message.ServerMessage;

public class SkipPacket implements ServerMessage {

  private final boolean ending;

  public SkipPacket(boolean ending) {
    this.ending = ending;
  }

  public static SkipPacket decode(boolean ending) {
    return new SkipPacket(ending);
  }

  @Override
  public boolean ending() {
    return this.ending;
  }

  public boolean resultSetEnd() {
    return this.ending;
  }

  public Sequencer getSequencer() {
    return null;
  }
}

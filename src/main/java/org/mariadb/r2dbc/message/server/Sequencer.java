// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import org.mariadb.r2dbc.message.MessageSequence;

public class Sequencer implements MessageSequence {
  private byte sequenceId;

  public Sequencer(byte sequenceId) {
    this.sequenceId = sequenceId;
  }

  public void reset() {
    sequenceId = (byte) 0xff;
  }

  public byte next() {
    return ++sequenceId;
  }
}

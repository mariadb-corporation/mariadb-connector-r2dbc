// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import org.mariadb.r2dbc.message.MessageSequence;

public class Sequencer implements MessageSequence {
  private byte sequenceId;

  public Sequencer(byte sequenceId) {
    this.sequenceId = sequenceId;
  }

  public byte next() {
    return ++sequenceId;
  }
}

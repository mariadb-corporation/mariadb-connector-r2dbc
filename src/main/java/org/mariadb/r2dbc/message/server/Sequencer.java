// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

public class Sequencer {
  private byte sequenceId;

  public Sequencer(byte sequenceId) {
    this.sequenceId = sequenceId;
  }

  public byte next() {
    return ++sequenceId;
  }
}

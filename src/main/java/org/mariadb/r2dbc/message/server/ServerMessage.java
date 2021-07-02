// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

public interface ServerMessage {
  default Sequencer getSequencer() {
    return null;
  }

  default boolean ending() {
    return false;
  }

  default boolean resultSetEnd() {
    return false;
  }
}

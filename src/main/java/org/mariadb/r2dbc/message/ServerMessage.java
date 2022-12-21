// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message;

public interface ServerMessage {

  default boolean ending() {
    return false;
  }

  default boolean resultSetEnd() {
    return false;
  }

  default int refCnt() {
    return -1000;
  }

  default boolean release() {
    return true;
  }
}

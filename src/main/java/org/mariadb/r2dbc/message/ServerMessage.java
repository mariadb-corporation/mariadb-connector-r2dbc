// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message;

public interface ServerMessage {

  default boolean ending() {
    return false;
  }

  default boolean resultSetEnd() {
    return false;
  }
}

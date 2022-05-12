// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util.constants;

public class StateChange {
  public static final short SESSION_TRACK_SYSTEM_VARIABLES = 0;
  public static final short SESSION_TRACK_SCHEMA = 1;
  public static final short SESSION_TRACK_STATE_CHANGE = 2;
  public static final short SESSION_TRACK_GTIDS = 3;
  public static final short SESSION_TRACK_TRANSACTION_CHARACTERISTICS = 4;
  public static final short SESSION_TRACK_TRANSACTION_STATE = 5;
}

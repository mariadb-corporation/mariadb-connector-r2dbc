// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message;

public interface AuthSwitch {

  String getPlugin();

  byte[] getSeed();

  MessageSequence getSequencer();
}

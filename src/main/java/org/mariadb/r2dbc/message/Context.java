// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message;

import io.r2dbc.spi.IsolationLevel;
import org.mariadb.r2dbc.client.ServerVersion;

public interface Context {

  long getThreadId();

  long getServerCapabilities();

  long getClientCapabilities();

  short getServerStatus();

  void setServerStatus(short serverStatus);

  IsolationLevel getIsolationLevel();

  void setIsolationLevel(IsolationLevel isolationLevel);

  String getDatabase();

  void setDatabase(String database);

  ServerVersion getVersion();
}

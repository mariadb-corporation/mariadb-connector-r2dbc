// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.util.ServerPrepareResult;

public final class CompletePrepareResult implements ServerMessage {

  private final ServerPrepareResult prepare;
  private boolean continueOnEnd;

  public CompletePrepareResult(final ServerPrepareResult prepare, boolean continueOnEnd) {
    this.prepare = prepare;
    this.continueOnEnd = continueOnEnd;
  }

  @Override
  public boolean ending() {
    return !continueOnEnd;
  }

  public ServerPrepareResult getPrepare() {
    return prepare;
  }
}

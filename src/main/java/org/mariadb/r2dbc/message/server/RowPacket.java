// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.message.ServerMessage;

public final class RowPacket implements ServerMessage {

  private final ByteBuf raw;

  public RowPacket(ByteBuf raw) {
    this.raw = raw.retain();
  }

  public ByteBuf getRaw() {
    return raw;
  }

  public void release() {
    raw.release();
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;

public final class RowPacket implements ServerMessage {

  private ByteBuf raw;

  public RowPacket(ByteBuf raw) {
    this.raw = raw.retain();
  }

  public ByteBuf getRaw() {
    return raw;
  }
}

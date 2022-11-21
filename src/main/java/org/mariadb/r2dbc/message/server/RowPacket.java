// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import org.mariadb.r2dbc.message.ServerMessage;

public final class RowPacket extends AbstractReferenceCounted implements ServerMessage {

  private final ByteBuf raw;

  public RowPacket(ByteBuf raw) {
    this.raw = raw.retain();
  }

  public ByteBuf getRaw() {
    return raw;
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    return raw.touch(hint);
  }

  public boolean release() {
    return raw.release();
  }

  @Override
  protected void deallocate() {}
}

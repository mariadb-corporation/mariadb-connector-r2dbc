// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import org.mariadb.r2dbc.message.ServerMessage;

public final class RowPacket implements ServerMessage, ReferenceCounted {

  private final ByteBuf raw;

  public RowPacket(ByteBuf raw) {
    this.raw = raw.retain();
  }

  public ByteBuf getRaw() {
    return raw;
  }

  @Override
  public int refCnt() {
    return raw.refCnt();
  }

  @Override
  public ReferenceCounted retain() {
    raw.retain();
    return this;
  }

  @Override
  public ReferenceCounted retain(int increment) {
    raw.retain(increment);
    return this;
  }

  @Override
  public ReferenceCounted touch() {
    raw.touch();
    return this;
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    raw.touch(hint);
    return this;
  }

  public boolean release() {
    return raw.release();
  }

  @Override
  public boolean release(int decrement) {
    return raw.release(decrement);
  }
}

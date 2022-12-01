// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

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
    return raw.retain();
  }

  @Override
  public ReferenceCounted retain(int increment) {
    return raw.retain(increment);
  }

  @Override
  public ReferenceCounted touch() {
    return raw.touch();
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    return raw.touch(hint);
  }

  public boolean release() {
    return raw.release();
  }

  @Override
  public boolean release(int decrement) {
    return raw.release(decrement);
  }
}

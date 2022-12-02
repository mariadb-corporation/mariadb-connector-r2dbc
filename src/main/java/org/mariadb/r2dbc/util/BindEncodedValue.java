// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import org.mariadb.r2dbc.codec.Codec;

public class BindEncodedValue implements ReferenceCounted {

  private final Codec<?> codec;
  private final ByteBuf value;

  public BindEncodedValue(Codec<?> codec, ByteBuf value) {
    this.codec = codec;
    this.value = value;
  }

  public Codec<?> getCodec() {
    return codec;
  }

  public ByteBuf getValue() {
    return value;
  }

  @Override
  public int refCnt() {
    return value.refCnt();
  }

  @Override
  public ReferenceCounted retain() {
    return value.retain();
  }

  @Override
  public ReferenceCounted retain(int increment) {
    return value.retain(increment);
  }

  @Override
  public ReferenceCounted touch() {
    return value.touch();
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    return value.touch(hint);
  }

  @Override
  public boolean release() {
    return value != null ? value.release() : true;
  }

  @Override
  public boolean release(int decrement) {
    return value.release(decrement);
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.codec.Codec;

public class BindEncodedValue {

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
}

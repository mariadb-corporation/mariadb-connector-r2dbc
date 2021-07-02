// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.util.BufferUtils;

public class Parameter<T> {
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static final Parameter<?> NULL_PARAMETER =
      new Parameter(null, null) {
        @Override
        public void encodeText(ByteBuf out, Context context) {
          BufferUtils.writeAscii(out, "null");
        }

        @Override
        public DataType getBinaryEncodeType() {
          return DataType.VARCHAR;
        }

        @Override
        public boolean isNull() {
          return true;
        }
      };

  private final Codec<T> codec;
  private final T value;

  public Parameter(Codec<T> codec, T value) {
    this.codec = codec;
    this.value = value;
  }

  public void encodeText(ByteBuf out, Context context) {
    codec.encodeText(out, context, this.value);
  }

  public void encodeBinary(ByteBuf out, Context context) {
    codec.encodeBinary(out, context, this.value);
  }

  public DataType getBinaryEncodeType() {
    return codec.getBinaryEncodeType();
  }

  public boolean isNull() {
    return false;
  }

  @Override
  public String toString() {
    return "Parameter{codec=" + codec.getClass().getSimpleName() + ", value=" + value + '}';
  }
}

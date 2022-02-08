// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public interface Codec<T> {

  boolean canDecode(ColumnDefinitionPacket column, Class<?> type);

  boolean canEncode(Class<?> value);

  T decodeText(
      ByteBuf buffer,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends T> type,
      ExceptionFactory factory);

  void encodeText(ByteBuf buf, Context context, Object value, ExceptionFactory factory);

  T decodeBinary(
      ByteBuf buffer,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends T> type,
      ExceptionFactory factory);

  void encodeBinary(ByteBuf buf, Context context, Object value, ExceptionFactory factory);

  DataType getBinaryEncodeType();
}

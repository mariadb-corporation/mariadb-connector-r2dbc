// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import reactor.core.publisher.Mono;

public interface Codec<T> {

  boolean canDecode(ColumnDefinitionPacket column, Class<?> type);

  boolean canEncode(Class<?> value);

  T decodeText(
      ByteBuf buffer,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends T> type,
      ExceptionFactory factory);

  T decodeBinary(
      ByteBuf buffer,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends T> type,
      ExceptionFactory factory);

  default Mono<ByteBuf> encodeText(ByteBufAllocator allocator, Object value, Context context) {
    throw new IllegalStateException("Not expected to be use");
  }

  default Mono<ByteBuf> encodeBinary(ByteBufAllocator allocator, Object value) {
    throw new IllegalStateException("Not expected to be use");
  }

  default void encodeDirectText(ByteBuf out, Object value, Context context) {
    throw new IllegalStateException("Not expected to be use");
  }

  default void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    throw new IllegalStateException("Not expected to be use");
  }

  DataType getBinaryEncodeType();

  default boolean isDirect() {
    return true;
  }
}

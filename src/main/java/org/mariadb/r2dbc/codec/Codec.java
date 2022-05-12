// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.function.Supplier;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BindValue;
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

  BindValue encodeText(
      ByteBufAllocator allocator, Object value, Context context, ExceptionFactory factory);

  BindValue encodeBinary(ByteBufAllocator allocator, Object value, ExceptionFactory factory);

  DataType getBinaryEncodeType();

  default BindValue createEncodedValue(Supplier<? extends ByteBuf> bufferSupplier) {
    return new BindValue(this, Mono.fromSupplier(bufferSupplier));
  }

  default BindValue createEncodedValue(Mono<? extends ByteBuf> value) {
    return new BindValue(this, value);
  }
}

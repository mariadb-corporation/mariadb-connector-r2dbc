// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.Clob;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BindValue;
import org.mariadb.r2dbc.util.BufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ClobCodec implements Codec<Clob> {

  public static final ClobCodec INSTANCE = new ClobCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.TEXT, DataType.VARSTRING, DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType()) && (type.isAssignableFrom(Clob.class));
  }

  public boolean canEncode(Class<?> value) {
    return Clob.class.isAssignableFrom(value);
  }

  @Override
  public Clob decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Clob> type,
      ExceptionFactory factory) {
    String rawValue = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    return Clob.from(Mono.just(rawValue));
  }

  @Override
  public Clob decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Clob> type,
      ExceptionFactory factory) {
    String rawValue = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    return Clob.from(Mono.just(rawValue));
  }

  @Override
  public BindValue encodeText(
      ByteBufAllocator allocator, Object value, Context context, ExceptionFactory factory) {
    return createEncodedValue(
        Flux.from(((Clob) value).stream())
            .reduce(new StringBuilder(), (a, b) -> a.append(b))
            .map(
                b ->
                    BufferUtils.encodeEscapedBytes(
                        allocator,
                        BufferUtils.STRING_PREFIX,
                        b.toString().getBytes(StandardCharsets.UTF_8),
                        context))
            .doOnSubscribe(e -> ((Clob) value).discard()));
  }

  @Override
  public BindValue encodeBinary(
      ByteBufAllocator allocator, Object value, ExceptionFactory factory) {
    return createEncodedValue(
        Flux.from(((Clob) value).stream())
            .reduce(new StringBuilder(), (a, b) -> a.append(b))
            .map(b -> BufferUtils.encodeLengthUtf8(allocator, b.toString()))
            .doOnSubscribe(e -> ((Clob) value).discard()));
  }

  public DataType getBinaryEncodeType() {
    return DataType.VARSTRING;
  }
}

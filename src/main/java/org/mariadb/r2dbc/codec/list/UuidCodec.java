// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.UUID;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class UuidCodec implements Codec<UUID> {

  public static final UuidCodec INSTANCE = new UuidCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.TEXT, DataType.VARSTRING, DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType()) && type.isAssignableFrom(UUID.class);
  }

  public boolean canEncode(Class<?> value) {
    return UUID.class.isAssignableFrom(value);
  }

  @Override
  public UUID decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends UUID> type,
      ExceptionFactory factory) {
    return UUID.fromString(buf.readCharSequence(length, StandardCharsets.UTF_8).toString());
  }

  @Override
  public UUID decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends UUID> type,
      ExceptionFactory factory) {
    return UUID.fromString(buf.readCharSequence(length, StandardCharsets.UTF_8).toString());
  }

  @Override
  public void encodeDirectText(ByteBuf out, Object value, Context context) {
    out.writeBytes(BufferUtils.STRING_PREFIX);
    byte[] b = value.toString().getBytes(StandardCharsets.UTF_8);
    BufferUtils.escapedBytes(out, b, b.length, context);
    out.writeByte('\'');
  }

  @Override
  public void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    byte[] b = value.toString().getBytes(StandardCharsets.UTF_8);
    out.writeBytes(BufferUtils.encodeLength(b.length));
    out.writeBytes(b);
  }

  public DataType getBinaryEncodeType() {
    return DataType.TEXT;
  }
}

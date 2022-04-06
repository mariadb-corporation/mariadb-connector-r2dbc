// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BindValue;
import org.mariadb.r2dbc.util.BufferUtils;

public class ByteArrayCodec implements Codec<byte[]> {

  public static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.GEOMETRY,
          DataType.VARSTRING,
          DataType.TEXT,
          DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Byte.TYPE && type.isArray())
            || type.isAssignableFrom(byte[].class));
  }

  @Override
  public byte[] decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends byte[]> type,
      ExceptionFactory factory) {

    byte[] arr = new byte[length];
    buf.readBytes(arr);
    return arr;
  }

  @Override
  public byte[] decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends byte[]> type,
      ExceptionFactory factory) {
    byte[] arr = new byte[length];
    buf.readBytes(arr);
    return arr;
  }

  public boolean canEncode(Class<?> value) {
    return byte[].class.isAssignableFrom(value);
  }

  @Override
  public BindValue encodeText(
      ByteBufAllocator allocator, Object value, Context context, ExceptionFactory factory) {
    return createEncodedValue(
        () ->
            BufferUtils.encodeEscapedBytes(
                allocator, BufferUtils.BINARY_PREFIX, (byte[]) value, context));
  }

  @Override
  public BindValue encodeBinary(
      ByteBufAllocator allocator, Object value, ExceptionFactory factory) {
    return createEncodedValue(() -> BufferUtils.encodeLengthBytes(allocator, (byte[]) value));
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}

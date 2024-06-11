// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class ByteBufferCodec implements Codec<ByteBuffer> {

  public static final ByteBufferCodec INSTANCE = new ByteBufferCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.STRING,
          DataType.VARSTRING,
          DataType.TEXT);

  private static ByteBuffer decode(
      ByteBuf buf, int length, ColumnDefinitionPacket column, ExceptionFactory factory) {
    switch (column.getDataType()) {
      case STRING:
      case TEXT:
      case VARSTRING:
        if (!column.isBinary()) {
          buf.skipBytes(length);
          throw factory.createParsingException(
              String.format(
                  "Data type %s (not binary) cannot be decoded as Blob", column.getDataType()));
        }
        ByteBuffer value = ByteBuffer.allocate(length);
        buf.readBytes(value);
        return value;

      default:
        // BIT, TINYBLOB, MEDIUMBLOB, LONGBLOB, BLOB, GEOMETRY
        byte[] val = new byte[length];
        buf.readBytes(val);
        return ByteBuffer.wrap(val);
    }
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(ByteBuffer.class);
  }

  @Override
  public ByteBuffer decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends ByteBuffer> type,
      ExceptionFactory factory) {
    return decode(buf, length, column, factory);
  }

  @Override
  public ByteBuffer decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends ByteBuffer> type,
      ExceptionFactory factory) {
    return decode(buf, length, column, factory);
  }

  public boolean canEncode(Class<?> value) {
    return ByteBuffer.class.isAssignableFrom(value);
  }

  @Override
  public void encodeDirectText(ByteBuf out, Object value, Context context) {
    out.writeBytes(BufferUtils.BINARY_PREFIX);
    ByteBuffer val = (ByteBuffer) value;
    if (val.hasArray()) {
      BufferUtils.escapedBytes(out, val.array(), val.remaining(), context);
    } else {
      byte[] arr = new byte[val.remaining()];
      val.get(arr);
      BufferUtils.escapedBytes(out, arr, arr.length, context);
    }
    out.writeByte('\'');
  }

  @Override
  public void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    ByteBuffer val = (ByteBuffer) value;
    out.writeBytes(BufferUtils.encodeLength(val.remaining()));
    out.writeBytes(val);
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}

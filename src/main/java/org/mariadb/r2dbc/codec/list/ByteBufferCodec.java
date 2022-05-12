// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BindValue;
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

  public boolean canEncode(Class<?> value) {
    return ByteBuf.class.isAssignableFrom(value);
  }

  @Override
  public BindValue encodeText(
      ByteBufAllocator allocator, Object value, Context context, ExceptionFactory factory) {
    return createEncodedValue(
        () -> {
          ByteBuffer val = (ByteBuffer) value;
          ByteBuf byteBuf = allocator.buffer();
          if (val.hasArray()) {
            BufferUtils.escapedBytes(byteBuf, val.array(), val.remaining(), context);
          } else {
            byte[] arr = new byte[val.remaining()];
            val.get(arr);
            BufferUtils.escapedBytes(byteBuf, arr, arr.length, context);
          }
          byteBuf.writeByte('\'');
          return byteBuf;
        });
  }

  @Override
  public BindValue encodeBinary(
      ByteBufAllocator allocator, Object value, ExceptionFactory factory) {
    return createEncodedValue(
        () -> {
          ByteBuffer val = (ByteBuffer) value;
          CompositeByteBuf compositeByteBuf = allocator.compositeBuffer();
          ByteBuf buf = Unpooled.wrappedBuffer(val);
          compositeByteBuf.addComponent(
              true, Unpooled.wrappedBuffer(BufferUtils.encodeLength(val.remaining())));
          compositeByteBuf.addComponent(true, buf);
          return compositeByteBuf;
        });
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}

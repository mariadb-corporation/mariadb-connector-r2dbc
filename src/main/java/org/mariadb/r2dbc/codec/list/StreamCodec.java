// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BindValue;
import org.mariadb.r2dbc.util.BufferUtils;

public class StreamCodec implements Codec<InputStream> {

  public static final StreamCodec INSTANCE = new StreamCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.TEXT,
          DataType.VARSTRING,
          DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(InputStream.class);
  }

  @Override
  public InputStream decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends InputStream> type,
      ExceptionFactory factory) {
    // STRING, VARCHAR, VARSTRING, BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB:
    return new ByteBufInputStream(buf.readRetainedSlice(length), true);
  }

  @Override
  public InputStream decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends InputStream> type,
      ExceptionFactory factory) {
    return new ByteBufInputStream(buf.readRetainedSlice(length), true);
  }

  public boolean canEncode(Class<?> value) {
    return InputStream.class.isAssignableFrom(value);
  }

  @Override
  public BindValue encodeText(
      ByteBufAllocator allocator, Object value, Context context, ExceptionFactory factory) {
    return createEncodedValue(
        () -> {
          ByteBuf buf = allocator.buffer();
          try {
            buf.writeBytes("_binary '".getBytes(StandardCharsets.US_ASCII));
            byte[] array = new byte[4096];
            int len;
            while ((len = ((InputStream) value).read(array)) > 0) {
              BufferUtils.escapedBytes(buf, array, len, context);
            }
            buf.writeByte('\'');
          } catch (IOException ioe) {
            throw factory.createParsingException("Failed to read InputStream", ioe);
          }
          return buf;
        });
  }

  @Override
  public BindValue encodeBinary(
      ByteBufAllocator allocator, Object value, ExceptionFactory factory) {
    return createEncodedValue(
        () -> {
          ByteBuf val = allocator.buffer();
          try {
            byte[] array = new byte[4096];
            int len;
            while ((len = ((InputStream) value).read(array)) > 0) {
              val.writeBytes(array, 0, len);
            }
          } catch (IOException ioe) {
            throw factory.createParsingException("Failed to read InputStream", ioe);
          }
          CompositeByteBuf compositeByteBuf = allocator.compositeBuffer();
          ByteBuf buf = Unpooled.wrappedBuffer(val);
          compositeByteBuf.addComponent(
              true, Unpooled.wrappedBuffer(BufferUtils.encodeLength(buf.readableBytes())));
          compositeByteBuf.addComponent(true, buf);
          return compositeByteBuf;
        });
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}

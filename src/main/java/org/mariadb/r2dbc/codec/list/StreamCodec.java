// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
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
  public void encodeDirectText(ByteBuf out, Object value, Context context) {

    try {
      out.writeBytes("_binary '".getBytes(StandardCharsets.US_ASCII));
      byte[] array = new byte[4096];
      int len;
      while ((len = ((InputStream) value).read(array)) > 0) {
        BufferUtils.escapedBytes(out, array, len, context);
      }
      out.writeByte('\'');
    } catch (IOException ioe) {
      throw new R2dbcNonTransientResourceException(
          "Failed to read InputStream", "H1000", 9000, ioe);
    }
  }

  @Override
  public void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    ByteBuf val = allocator.buffer();
    try {
      byte[] array = new byte[4096];
      int len;
      while ((len = ((InputStream) value).read(array)) > 0) {
        val.writeBytes(array, 0, len);
      }
    } catch (IOException ioe) {
      throw new R2dbcNonTransientResourceException(
          "Failed to read InputStream", "H1000", 9000, ioe);
    }
    out.writeBytes(BufferUtils.encodeLength(val.readableBytes()));
    out.writeBytes(val);
    val.release();
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}

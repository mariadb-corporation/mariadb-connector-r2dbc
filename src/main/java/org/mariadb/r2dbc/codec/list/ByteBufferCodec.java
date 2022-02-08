// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
  public void encodeText(ByteBuf buf, Context context, Object value, ExceptionFactory factory) {
    buf.writeBytes("_binary '".getBytes(StandardCharsets.US_ASCII));

    ByteBuffer tempVal = ((ByteBuffer) value);
    if (tempVal.hasArray()) {
      BufferUtils.writeEscaped(
          buf, tempVal.array(), tempVal.arrayOffset(), tempVal.remaining(), context);
    } else {
      byte[] intermediaryBuf = new byte[tempVal.remaining()];
      tempVal.get(intermediaryBuf);
      BufferUtils.writeEscaped(buf, intermediaryBuf, 0, intermediaryBuf.length, context);
    }
    buf.writeByte((byte) '\'');
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Object value, ExceptionFactory factory) {
    buf.writeByte(0xfe);
    int initialPos = buf.writerIndex();
    buf.writerIndex(buf.writerIndex() + 8); // reserve length encoded length bytes
    ByteBuffer tempVal = ((ByteBuffer) value);
    buf.writeBytes(tempVal);
    int endPos = buf.writerIndex();
    buf.writerIndex(initialPos);
    buf.writeLongLE(endPos - (initialPos + 8));
    buf.writerIndex(endPos);
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}

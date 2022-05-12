// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.BitSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BindValue;
import org.mariadb.r2dbc.util.BufferUtils;

public class BitSetCodec implements Codec<BitSet> {

  public static final BitSetCodec INSTANCE = new BitSetCodec();

  public static BitSet parseBit(ByteBuf buf, int length) {
    byte[] arr = new byte[length];
    buf.readBytes(arr);
    revertOrder(arr);
    return BitSet.valueOf(arr);
  }

  public static void revertOrder(byte[] array) {
    int i = 0;
    int j = array.length - 1;
    byte tmp;
    while (j > i) {
      tmp = array[j];
      array[j] = array[i];
      array[i] = tmp;
      j--;
      i++;
    }
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getDataType() == DataType.BIT && type.isAssignableFrom(BitSet.class);
  }

  @Override
  public BitSet decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends BitSet> type,
      ExceptionFactory factory) {
    return parseBit(buf, length);
  }

  @Override
  public BitSet decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends BitSet> type,
      ExceptionFactory factory) {
    return parseBit(buf, length);
  }

  public boolean canEncode(Class<?> value) {
    return BitSet.class.isAssignableFrom(value);
  }

  @Override
  public BindValue encodeText(
      ByteBufAllocator allocator, Object value, Context context, ExceptionFactory factory) {
    return createEncodedValue(
        () -> {
          byte[] bytes = ((BitSet) value).toByteArray();
          revertOrder(bytes);

          StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
          sb.append("b'");
          for (int i = 0; i < Byte.SIZE * bytes.length; i++)
            sb.append((bytes[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
          sb.append("'");
          return BufferUtils.encodeAscii(allocator, sb.toString());
        });
  }

  @Override
  public BindValue encodeBinary(
      ByteBufAllocator allocator, Object value, ExceptionFactory factory) {
    return createEncodedValue(
        () -> {
          byte[] bytes = ((BitSet) value).toByteArray();
          revertOrder(bytes);
          return BufferUtils.encodeLengthBytes(allocator, bytes);
        });
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}

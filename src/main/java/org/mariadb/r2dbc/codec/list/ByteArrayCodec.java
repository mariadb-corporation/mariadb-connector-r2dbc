/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class ByteArrayCodec implements Codec<byte[]> {

  public static final byte[] BINARY_PREFIX = {'_', 'b', 'i', 'n', 'a', 'r', 'y', ' ', '\''};

  public static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.BIT,
          DataType.GEOMETRY,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING);

  public String className() {
    return byte[].class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE && type.isArray())
            || type.isAssignableFrom(byte[].class));
  }

  @Override
  public byte[] decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends byte[]> type) {
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case STRING:
      case VARSTRING:
      case VARCHAR:
        byte[] arr = new byte[length];
        buf.readBytes(arr);
        return arr;

      default:
        buf.skipBytes(length);
        throw new R2dbcNonTransientResourceException(
            String.format("Data type %s cannot be decoded as byte[]", column.getType()));
    }
  }

  @Override
  public byte[] decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends byte[]> type) {
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case STRING:
      case VARSTRING:
      case VARCHAR:
        byte[] arr = new byte[length];
        buf.readBytes(arr);
        return arr;

      default:
        buf.skipBytes(length);
        throw new R2dbcNonTransientResourceException(
            String.format("Data type %s cannot be decoded as byte[]", column.getType()));
    }
  }

  public boolean canEncode(Object value) {
    return value instanceof byte[];
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, byte[] value) {
    buf.writeBytes(BINARY_PREFIX);
    BufferUtils.writeEscaped(buf, value, 0, value.length, context);
    buf.writeByte('\'');
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, byte[] value) {

    BufferUtils.writeLengthEncode(value.length, buf);
    buf.writeBytes(value);
  }

  @Override
  public void encodeLongData(ByteBuf buf, Context context, byte[] value) {
    buf.writeBytes(value);
  }

  public boolean canEncodeLongData() {
    return true;
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }

  @Override
  public String toString() {
    return "ByteArrayCodec{}";
  }
}

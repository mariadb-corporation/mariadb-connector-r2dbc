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
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class ShortCodec implements Codec<Short> {

  public static final ShortCodec INSTANCE = new ShortCodec();

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return (column.getDataType() == DataType.TINYINT
            || (column.getDataType() == DataType.SMALLINT && column.isSigned())
            || column.getDataType() == DataType.YEAR
            || column.getDataType() == DataType.FLOAT
            || column.getDataType() == DataType.DOUBLE)
        && ((type.isPrimitive() && type == Short.TYPE) || type.isAssignableFrom(Short.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Short;
  }

  @Override
  public Short decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Short> type) {
    switch (column.getDataType()) {
      case DOUBLE:
        String str = buf.readCharSequence(length, StandardCharsets.US_ASCII).toString();
        return Double.valueOf(str).shortValue();

      case FLOAT:
        String str2 = buf.readCharSequence(length, StandardCharsets.US_ASCII).toString();
        return Float.valueOf(str2).shortValue();

      default:
        return (short) LongCodec.parse(buf, length);
    }
  }

  @Override
  public Short decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Short> type) {
    switch (column.getDataType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return buf.readUnsignedByte();
        }
        return (short) buf.readByte();

      case DOUBLE:
        return (short) buf.readDoubleLE();

      case FLOAT:
        return (short) buf.readFloatLE();

      default: // YEAR and SMALLINT
        return buf.readShortLE();
    }
  }

  @Override
  public void encodeText(ByteBuf buf, ConnectionContext context, Short value) {
    BufferUtils.writeAscii(buf, String.valueOf(value));
  }

  @Override
  public void encodeBinary(ByteBuf buf, ConnectionContext context, Short value) {
    buf.writeShortLE(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.SMALLINT;
  }

  @Override
  public String toString() {
    return "ShortCodec{}";
  }
}

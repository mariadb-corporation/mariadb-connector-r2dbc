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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class IntCodec implements Codec<Integer> {

  public static final IntCodec INSTANCE = new IntCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.YEAR,
          DataType.DECIMAL);

  public static void rangeCheck(
      String className, long minValue, long maxValue, long value, ColumnDefinitionPacket col) {
    if (value < minValue || value > maxValue) {
      throw new IllegalArgumentException(
          String.format(
              "Out of range value for column '%s' : value %d  is not in %s range",
              col.getColumnAlias(), value, className));
    }
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Integer.TYPE) || type.isAssignableFrom(Integer.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Integer;
  }

  @Override
  public Integer decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Integer> type) {
    long result;
    switch (column.getDataType()) {
      case BIT:
        return (int) ByteCodec.parseBit(buf, length);
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        result = LongCodec.parse(buf, length);
        break;
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
        String str = buf.readCharSequence(length, StandardCharsets.US_ASCII).toString();
        try {
          result = new BigDecimal(str).longValue();
          break;
        } catch (NumberFormatException nfe) {
          throw new IllegalArgumentException(String.format("Incorrect format %s", str));
        }
      default:
        buf.skipBytes(length);
        throw new IllegalArgumentException(
            String.format("Unexpected datatype %s", column.getDataType()));
    }
    rangeCheck(Integer.class.getName(), Integer.MIN_VALUE, Integer.MAX_VALUE, result, column);
    return (int) result;
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, Integer value) {
    BufferUtils.writeAscii(buf, String.valueOf(value));
  }

  @Override
  public String toString() {
    return "IntCodec{}";
  }
}

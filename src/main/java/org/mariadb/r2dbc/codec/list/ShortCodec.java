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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class ShortCodec implements Codec<Short> {

  public static final ShortCodec INSTANCE = new ShortCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.VARCHAR,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.BIT,
          DataType.YEAR);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Short.TYPE) || type.isAssignableFrom(Short.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Short;
  }

  public String className() {
    return Short.class.getName();
  }

  @Override
  public Short decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Short> type) {
    long result;
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        result = LongCodec.parse(buf, length);
        break;

      case BIT:
        result = 0;
        for (int i = 0; i < Math.min(length, 8); i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        if (length > 8) {
          buf.skipBytes(length - 8);
        }
        break;

      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case VARCHAR:
      case DECIMAL:
      case ENUM:
      case VARSTRING:
      case STRING:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
          break;
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as Short", str));
        }

      default:
        buf.skipBytes(length);
        throw new R2dbcNonTransientResourceException(
            String.format("Data type %s cannot be decoded as Short", column.getType()));
    }

    if ((short) result != result || (result < 0 && !column.isSigned())) {
      throw new R2dbcNonTransientResourceException("Short overflow");
    }

    return (short) result;
  }

  @Override
  public Short decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Short> type) {

    long result;
    switch (column.getType()) {
      case TINYINT:
        result = column.isSigned() ? buf.readByte() : buf.readUnsignedByte();
        break;

      case YEAR:
      case SMALLINT:
        result = column.isSigned() ? buf.readShortLE() : buf.readUnsignedShortLE();
        break;

      case MEDIUMINT:
        result = column.isSigned() ? buf.readMediumLE() : buf.readUnsignedMediumLE();
        break;

      case INTEGER:
        result = column.isSigned() ? buf.readIntLE() : buf.readUnsignedIntLE();
        break;

      case BIGINT:
        result = buf.readLongLE();
        if (result < 0 & !column.isSigned()) {
          throw new R2dbcNonTransientResourceException("Short overflow");
        }
        break;

      case BIT:
        result = 0;
        for (int i = 0; i < Math.min(length, 8); i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        if (length > 8) {
          buf.skipBytes(length - 8);
        }
        break;

      case FLOAT:
        result = (long) buf.readFloatLE();
        break;

      case DOUBLE:
        result = (long) buf.readDoubleLE();
        break;

      case OLDDECIMAL:
      case VARCHAR:
      case DECIMAL:
      case ENUM:
      case VARSTRING:
      case STRING:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
          break;
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as Short", str));
        }

      default:
        buf.skipBytes(length);
        throw new R2dbcNonTransientResourceException(
            String.format("Data type %s cannot be decoded as Short", column.getType()));
    }

    if ((short) result != result || (result < 0 && !column.isSigned())) {
      throw new R2dbcNonTransientResourceException("Short overflow");
    }

    return (short) result;
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Short value) {
    BufferUtils.writeAscii(buf, String.valueOf(value));
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Short value) {
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

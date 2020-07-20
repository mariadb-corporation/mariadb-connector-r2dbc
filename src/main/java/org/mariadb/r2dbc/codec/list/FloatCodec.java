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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class FloatCodec implements Codec<Float> {

  public static final FloatCodec INSTANCE = new FloatCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.BIGINT,
          DataType.OLDDECIMAL,
          DataType.DECIMAL,
          DataType.YEAR,
          DataType.DOUBLE,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Float.TYPE) || type.isAssignableFrom(Float.class));
  }

  public boolean canEncode(Class value) {
    return Float.class.isAssignableFrom(value);
  }

  @Override
  public Float decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Float> type) {
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case DOUBLE:
      case OLDDECIMAL:
      case DECIMAL:
      case YEAR:
      case FLOAT:
        return Float.valueOf(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());

      default:
        // VARCHAR, VARSTRING, STRING:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return Float.valueOf(val);
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as Float", val));
        }
    }
  }

  @Override
  public Float decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Float> type) {

    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return (float) buf.readUnsignedByte();
        }
        return (float) buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return (float) buf.readUnsignedShortLE();
        }
        return (float) buf.readShortLE();

      case MEDIUMINT:
        if (!column.isSigned()) {
          return (float) buf.readUnsignedMediumLE();
        }
        return (float) buf.readMediumLE();

      case INTEGER:
        if (!column.isSigned()) {
          return (float) buf.readUnsignedIntLE();
        }
        return (float) buf.readIntLE();

      case BIGINT:
        if (column.isSigned()) {
          return (float) buf.readLongLE();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          return new BigInteger(1, bb).floatValue();
        }

      case DOUBLE:
        return (float) buf.readDoubleLE();

      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString())
            .floatValue();

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str2 = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return Float.valueOf(str2);
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as Float", str2));
        }

      default:
        // FLOAT
        return buf.readFloatLE();
    }
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Float value) {
    BufferUtils.writeAscii(buf, String.valueOf(value));
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Float value) {
    buf.writeFloatLE(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.FLOAT;
  }
}

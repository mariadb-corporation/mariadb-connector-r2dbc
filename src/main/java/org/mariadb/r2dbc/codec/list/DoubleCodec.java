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
import java.util.EnumSet;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class DoubleCodec implements Codec<Double> {

  public static final DoubleCodec INSTANCE = new DoubleCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.OLDDECIMAL,
          DataType.DECIMAL,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING);

  public String className() {
    return Double.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Double.TYPE) || type.isAssignableFrom(Double.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Double;
  }

  @Override
  public Double decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Double> type) {
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case DECIMAL:
        return Double.valueOf(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str2 = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return Double.valueOf(str2);
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as Double", str2));
        }

      default:
        buf.skipBytes(length);
        throw new R2dbcNonTransientResourceException(
            String.format("Data type %s cannot be decoded as Double", column.getType()));
    }
  }

  @Override
  public Double decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Double> type) {
    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return (double) buf.readUnsignedByte();
        }
        return (double) buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return (double) buf.readUnsignedShortLE();
        }
        return (double) buf.readShortLE();

      case MEDIUMINT:
        if (!column.isSigned()) {
          return (double) buf.readUnsignedMediumLE();
        }
        return (double) buf.readMediumLE();

      case INTEGER:
        if (!column.isSigned()) {
          return (double) buf.readUnsignedIntLE();
        }
        return (double) buf.readIntLE();

      case BIGINT:
        if (column.isSigned()) {
          return (double) buf.readLongLE();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          return new BigInteger(1, bb).doubleValue();
        }

      case FLOAT:
        return (double) buf.readFloatLE();

      case DOUBLE:
        return buf.readDoubleLE();

      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString())
            .doubleValue();

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str2 = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return Double.valueOf(str2);
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as Double", str2));
        }

      default:
        buf.skipBytes(length);
        throw new R2dbcNonTransientResourceException(
            String.format("Data type %s cannot be decoded as Double", column.getType()));
    }
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Double value) {
    BufferUtils.writeAscii(buf, String.valueOf(value));
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Double value) {
    buf.writeDoubleLE(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.DOUBLE;
  }

  @Override
  public String toString() {
    return "DoubleCodec{}";
  }
}

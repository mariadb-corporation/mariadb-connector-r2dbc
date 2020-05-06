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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class DoubleCodec implements Codec<Double> {

  public static final DoubleCodec INSTANCE = new DoubleCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
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
          DataType.DECIMAL);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Double.TYPE) || type.isAssignableFrom(Double.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Double;
  }

  @Override
  public Double decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Double> type) {
    String str = buf.readCharSequence(length, StandardCharsets.US_ASCII).toString();
    return Double.valueOf(str);
  }

  @Override
  public Double decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Double> type) {
    switch (column.getDataType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return Double.valueOf(buf.readUnsignedByte());
        }
        return Double.valueOf((int) buf.readByte());

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return Double.valueOf(buf.readUnsignedShortLE());
        }
        return Double.valueOf((int) buf.readShortLE());

      case MEDIUMINT:
        if (!column.isSigned()) {
          return Double.valueOf((buf.readUnsignedMediumLE()));
        }
        return Double.valueOf(buf.readMediumLE());

      case INTEGER:
        if (!column.isSigned()) {
          return Double.valueOf(buf.readUnsignedIntLE());
        }
        return Double.valueOf(buf.readIntLE());

      case BIGINT:
        BigInteger val;
        if (column.isSigned()) {
          val = BigInteger.valueOf(buf.readLongLE());
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          val = new BigInteger(1, bb);
        }
        return val.doubleValue();

      case FLOAT:
        return Double.valueOf(buf.readFloatLE());

      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.UTF_8).toString())
            .doubleValue();
      default:
        return buf.readDoubleLE();
    }
  }

  @Override
  public void encodeText(ByteBuf buf, ConnectionContext context, Double value) {
    BufferUtils.writeAscii(buf, String.valueOf(value));
  }

  @Override
  public void encodeBinary(ByteBuf buf, ConnectionContext context, Double value) {
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

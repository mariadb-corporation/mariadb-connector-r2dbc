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

public class BigDecimalCodec implements Codec<BigDecimal> {

  public static final BigDecimalCodec INSTANCE = new BigDecimalCodec();

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
          DataType.DECIMAL);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(BigDecimal.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof BigDecimal;
  }

  @Override
  public BigDecimal decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BigDecimal> type) {
    String value = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    return new BigDecimal(value);
  }

  @Override
  public BigDecimal decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BigDecimal> type) {

    switch (column.getDataType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return BigDecimal.valueOf(buf.readUnsignedByte());
        }
        return BigDecimal.valueOf((int) buf.readByte());

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return BigDecimal.valueOf(buf.readUnsignedShortLE());
        }
        return BigDecimal.valueOf((int) buf.readShortLE());

      case MEDIUMINT:
        if (!column.isSigned()) {
          return BigDecimal.valueOf((buf.readUnsignedMediumLE()));
        }
        return BigDecimal.valueOf(buf.readMediumLE());

      case INTEGER:
        if (!column.isSigned()) {
          return BigDecimal.valueOf(buf.readUnsignedIntLE());
        }
        return BigDecimal.valueOf(buf.readIntLE());

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

        return new BigDecimal(String.valueOf(val)).setScale(column.getDecimals());

      case FLOAT:
        return BigDecimal.valueOf(buf.readFloatLE());

      case DOUBLE:
        return BigDecimal.valueOf(buf.readDoubleLE());

      default:
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.UTF_8).toString());
    }
  }

  @Override
  public void encodeText(ByteBuf buf, ConnectionContext context, BigDecimal value) {
    BufferUtils.writeAscii(buf, value.toPlainString());
  }

  @Override
  public void encodeBinary(ByteBuf buf, ConnectionContext context, BigDecimal value) {
    String asciiFormat = value.toPlainString();
    BufferUtils.writeLengthEncode(asciiFormat.length(), buf);
    BufferUtils.writeAscii(buf, asciiFormat);
  }

  public DataType getBinaryEncodeType() {
    return DataType.DECIMAL;
  }

  @Override
  public String toString() {
    return "BigDecimalCodec{}";
  }
}

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

public class BigIntegerCodec implements Codec<BigInteger> {

  public static final BigIntegerCodec INSTANCE = new BigIntegerCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.DECIMAL,
          DataType.YEAR,
          DataType.DOUBLE,
          DataType.FLOAT);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(BigInteger.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof BigInteger;
  }

  @Override
  public BigInteger decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BigInteger> type) {
    switch (column.getDataType()) {
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
        String value = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        return new BigDecimal(value).toBigInteger();

      default:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        return new BigInteger(val);
    }
  }

  @Override
  public BigInteger decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BigInteger> type) {

    switch (column.getDataType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedByte());
        }
        return BigInteger.valueOf((int) buf.readByte());

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedShortLE());
        }
        return BigInteger.valueOf((int) buf.readShortLE());

      case MEDIUMINT:
        if (!column.isSigned()) {
          return BigInteger.valueOf((buf.readUnsignedMediumLE()));
        }
        return BigInteger.valueOf(buf.readMediumLE());

      case INTEGER:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedIntLE());
        }
        return BigInteger.valueOf(buf.readIntLE());

      case FLOAT:
        return BigDecimal.valueOf(buf.readFloatLE()).toBigInteger();

      case DOUBLE:
        return BigDecimal.valueOf(buf.readDoubleLE()).toBigInteger();

      case DECIMAL:
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.UTF_8).toString())
            .toBigInteger();

      default:
        if (column.isSigned()) return BigInteger.valueOf(buf.readLongLE());

        // need BIG ENDIAN, so reverse order
        byte[] bb = new byte[8];
        for (int i = 7; i >= 0; i--) {
          bb[i] = buf.readByte();
        }
        return new BigInteger(1, bb);
    }
  }

  @Override
  public void encodeText(ByteBuf buf, ConnectionContext context, BigInteger value) {
    BufferUtils.writeAscii(buf, value.toString());
  }

  @Override
  public void encodeBinary(ByteBuf buf, ConnectionContext context, BigInteger value) {
    String asciiFormat = value.toString();
    BufferUtils.writeLengthEncode(asciiFormat.length(), buf);
    BufferUtils.writeAscii(buf, asciiFormat);
  }

  public DataType getBinaryEncodeType() {
    return DataType.DECIMAL;
  }

  @Override
  public String toString() {
    return "BigIntegerCodec{}";
  }
}

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

public class BigIntegerCodec implements Codec<BigInteger> {

  public static BigIntegerCodec INSTANCE = new BigIntegerCodec();
  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.DECIMAL,
          DataType.YEAR,
          DataType.DOUBLE,
          DataType.DECIMAL,
          DataType.OLDDECIMAL,
          DataType.FLOAT,
          DataType.BIT,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(BigInteger.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof BigInteger;
  }

  @Override
  public BigInteger decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BigInteger> type) {

    switch (column.getType()) {
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case OLDDECIMAL:
        String value = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        return new BigDecimal(value).toBigInteger();

      case BIT:
        long result = 0;
        for (int i = 0; i < Math.min(length, 8); i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        if (length > 8) {
          buf.skipBytes(length - 8);
        }
        return BigInteger.valueOf(result);

      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        return new BigInteger(buf.readCharSequence(length, StandardCharsets.UTF_8).toString());

      default:
        // VARCHAR, VARSTRING, STRING
        String str2 = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return new BigDecimal(str2).toBigInteger();
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as BigInteger", str2));
        }
    }
  }

  @Override
  public BigInteger decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BigInteger> type) {

    switch (column.getType()) {
      case BIT:
        long result = 0;
        for (int i = 0; i < Math.min(length, 8); i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        if (length > 8) {
          buf.skipBytes(length - 8);
        }
        return BigInteger.valueOf(result);
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

      case BIGINT:
        if (column.isSigned()) return BigInteger.valueOf(buf.readLongLE());

        // need BIG ENDIAN, so reverse order
        byte[] bb = new byte[8];
        for (int i = 7; i >= 0; i--) {
          bb[i] = buf.readByte();
        }
        return new BigInteger(1, bb);

      default:
        // VARCHAR, VARSTRING, STRING
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return new BigInteger(str);
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as BigInteger", str));
        }
    }
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, BigInteger value) {
    BufferUtils.writeAscii(buf, value.toString());
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, BigInteger value) {
    String asciiFormat = value.toString();
    BufferUtils.writeLengthEncode(asciiFormat.length(), buf);
    BufferUtils.writeAscii(buf, asciiFormat);
  }

  public DataType getBinaryEncodeType() {
    return DataType.DECIMAL;
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class IntCodec implements Codec<Integer> {

  public static final IntCodec INSTANCE = new IntCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.TEXT,
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
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Integer.TYPE) || type.isAssignableFrom(Integer.class));
  }

  public boolean canEncode(Class<?> value) {
    return Integer.class.isAssignableFrom(value);
  }

  @Override
  public Integer decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Integer> type) {
    long result;
    switch (column.getDataType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case YEAR:
        result = LongCodec.parse(buf, length);
        break;

      case BIGINT:
        result = LongCodec.parse(buf, length);
        if (result < 0 & !column.isSigned()) {
          throw new R2dbcNonTransientResourceException("integer overflow");
        }
        break;

      case BIT:
        result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        break;

      default:
        // FLOAT, DOUBLE, OLDDECIMAL, VARCHAR, DECIMAL, ENUM, VARSTRING, STRING:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
          break;
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as Integer", str));
        }
    }

    if ((int) result != result) {
      throw new R2dbcNonTransientResourceException("integer overflow");
    }

    return (int) result;
  }

  @Override
  public Integer decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Integer> type) {

    long result;
    switch (column.getDataType()) {
      case INTEGER:
        result = column.isSigned() ? buf.readIntLE() : buf.readUnsignedIntLE();
        break;

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

      case BIGINT:
        if (column.isSigned()) {
          result = buf.readLongLE();
          break;
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          BigInteger val = new BigInteger(1, bb);
          try {
            return val.intValueExact();
          } catch (ArithmeticException ae) {
            throw new R2dbcNonTransientResourceException("integer overflow");
          }
        }

      case BIT:
        result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        break;

      case FLOAT:
        result = (long) buf.readFloatLE();
        break;

      case DOUBLE:
        result = (long) buf.readDoubleLE();
        break;

      default:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
          break;
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as Integer", str));
        }
    }

    if ((int) result != result || (result < 0 && !column.isSigned())) {
      throw new R2dbcNonTransientResourceException("integer overflow");
    }

    return (int) result;
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Object value) {
    BufferUtils.writeAscii(buf, Integer.toString((int) value));
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Object value) {
    buf.writeIntLE((int) value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.INTEGER;
  }
}

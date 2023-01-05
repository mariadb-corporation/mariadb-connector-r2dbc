// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class LongCodec implements Codec<Long> {

  public static final LongCodec INSTANCE = new LongCodec();

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

  public static long parse(ByteBuf buf, int length) {
    long result = 0;
    boolean negate = false;
    int idx = 0;
    if (length > 0 && buf.getByte(buf.readerIndex()) == 45) { // minus sign
      negate = true;
      idx++;
      buf.skipBytes(1);
    }

    while (idx++ < length) {
      result = result * 10 + buf.readByte() - 48;
    }

    if (negate) result = -1 * result;
    return result;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Long.TYPE) || type.isAssignableFrom(Long.class));
  }

  public boolean canEncode(Class<?> value) {
    return Long.class.isAssignableFrom(value);
  }

  @Override
  public Long decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Long> type,
      ExceptionFactory factory) {
    long result;
    switch (column.getDataType()) {
      case DECIMAL:
      case OLDDECIMAL:
      case DOUBLE:
      case FLOAT:
        String str1 = buf.readCharSequence(length, StandardCharsets.US_ASCII).toString();
        try {
          return new BigDecimal(str1).setScale(0, RoundingMode.DOWN).longValueExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as Long", str1));
        }

      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case YEAR:
        return parse(buf, length);

      case BIGINT:
        if (column.isSigned()) {
          result = parse(buf, length);
          break;
        } else {
          BigInteger val =
              new BigInteger(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());
          try {
            return val.longValueExact();
          } catch (ArithmeticException ae) {
            throw factory.createParsingException(
                String.format("value '%s' cannot be decoded as Long", val));
          }
        }

      case BIT:
        result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return result;

      default:
        // STRING, VARCHAR, VARSTRING:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return new BigInteger(str).longValueExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as Long", str));
        }
    }

    return result;
  }

  @Override
  public Long decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Long> type,
      ExceptionFactory factory) {

    switch (column.getDataType()) {
      case BIGINT:
        if (column.isSigned()) {
          return buf.readLongLE();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          BigInteger val = new BigInteger(1, bb);
          try {
            return val.longValueExact();
          } catch (ArithmeticException ae) {
            throw factory.createParsingException(
                String.format("value '%s' cannot be decoded as Long", val));
          }
        }

      case BIT:
        long result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return result;

      case TINYINT:
        if (!column.isSigned()) {
          return (long) buf.readUnsignedByte();
        }
        return (long) buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return (long) buf.readUnsignedShortLE();
        }
        return (long) buf.readShortLE();

      case MEDIUMINT:
        long v = column.isSigned() ? buf.readMediumLE() : buf.readUnsignedMediumLE();
        buf.readByte(); // needed since binary protocol exchange for medium are on 4 bytes
        return v;

      case INTEGER:
        if (!column.isSigned()) {
          return buf.readUnsignedIntLE();
        }
        return (long) buf.readIntLE();

      case FLOAT:
        return (long) buf.readFloatLE();

      case DOUBLE:
        return (long) buf.readDoubleLE();

      default:
        // VARSTRING, VARCHAR, STRING, OLDDECIMAL, DECIMAL:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as Long", str));
        }
    }
  }

  @Override
  public void encodeDirectText(ByteBuf out, Object value, Context context) {
    out.writeCharSequence(value.toString(), StandardCharsets.US_ASCII);
  }

  @Override
  public void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    out.writeLongLE((Long) value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.BIGINT;
  }
}

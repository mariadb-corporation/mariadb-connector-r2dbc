// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class BigIntegerCodec implements Codec<BigInteger> {

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
          DataType.TEXT,
          DataType.VARSTRING,
          DataType.STRING);
  public static BigIntegerCodec INSTANCE = new BigIntegerCodec();

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(BigInteger.class);
  }

  public boolean canEncode(Class<?> value) {
    return BigInteger.class.isAssignableFrom(value);
  }

  @Override
  public BigInteger decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends BigInteger> type,
      ExceptionFactory factory) {

    switch (column.getDataType()) {
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case OLDDECIMAL:
        String value = buf.readCharSequence(length, StandardCharsets.US_ASCII).toString();
        return new BigDecimal(value).toBigInteger();

      case BIT:
        long result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return BigInteger.valueOf(result);

      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        return new BigInteger(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());

      default:
        // VARCHAR, VARSTRING, STRING
        String str2 = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return new BigDecimal(str2).toBigIntegerExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as BigInteger", str2));
        }
    }
  }

  @Override
  public BigInteger decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends BigInteger> type,
      ExceptionFactory factory) {

    switch (column.getDataType()) {
      case BIT:
        long result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
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
        BigInteger v =
            BigInteger.valueOf(column.isSigned() ? buf.readMediumLE() : buf.readUnsignedMediumLE());
        buf.readByte(); // needed since binary protocol exchange for medium are on 4 bytes
        return v;

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
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as BigInteger", str));
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
    String v = value.toString();
    out.writeBytes(BufferUtils.encodeLength(v.length()));
    out.writeCharSequence(v, StandardCharsets.US_ASCII);
  }

  public DataType getBinaryEncodeType() {
    return DataType.DECIMAL;
  }
}

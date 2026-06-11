// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class BigDecimalCodec implements Codec<BigDecimal> {

  public static final BigDecimalCodec INSTANCE = new BigDecimalCodec();
  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIGINT,
          DataType.BIT,
          DataType.DECIMAL,
          DataType.OLDDECIMAL,
          DataType.YEAR,
          DataType.DECIMAL,
          DataType.TEXT,
          DataType.VARSTRING,
          DataType.STRING);

  /**
   * Maximum length of a numeric string the driver will parse into a {@link BigDecimal} (or {@link
   * BigInteger}).
   */
  public static final int MAX_BIG_DECIMAL_STRING_LENGTH = 1024;

  /**
   * Parse a numeric string into a {@link BigDecimal}, rejecting inputs longer than {@link
   * #MAX_BIG_DECIMAL_STRING_LENGTH} characters.
   *
   * @param str numeric text
   * @param factory exception factory used to report an over-cap input
   * @return parsed BigDecimal
   * @throws io.r2dbc.spi.R2dbcException if {@code str} is longer than the cap
   * @throws NumberFormatException if {@code str} isn't a valid number (caller may catch and rewrap
   *     with column-specific context)
   */
  public static BigDecimal parseBigDecimal(String str, ExceptionFactory factory) {
    if (str.length() > MAX_BIG_DECIMAL_STRING_LENGTH) {
      throw factory.createParsingException(
          "value cannot be decoded as BigDecimal: length "
              + str.length()
              + " exceeds maximum of "
              + MAX_BIG_DECIMAL_STRING_LENGTH
              + " characters");
    }
    return new BigDecimal(str);
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(BigDecimal.class);
  }

  public boolean canEncode(Class<?> value) {
    return BigDecimal.class.isAssignableFrom(value);
  }

  @Override
  public BigDecimal decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends BigDecimal> type,
      ExceptionFactory factory) {
    switch (column.getDataType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case YEAR:
      case DECIMAL:
      case OLDDECIMAL:
        return parseBigDecimal(
            buf.readCharSequence(length, StandardCharsets.UTF_8).toString(), factory);

      case BIT:
        long result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return BigDecimal.valueOf(result);

      default:
        // VARCHAR, VARSTRING, STRING
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return parseBigDecimal(str, factory);
        } catch (NumberFormatException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as BigDecimal", str));
        }
    }
  }

  @Override
  public BigDecimal decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends BigDecimal> type,
      ExceptionFactory factory) {

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
        BigDecimal v =
            BigDecimal.valueOf(column.isSigned() ? buf.readMediumLE() : buf.readUnsignedMediumLE());
        buf.readByte(); // needed since binary protocol exchange for medium are on 4 bytes
        return v;

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

      case BIT:
        long result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return BigDecimal.valueOf(result);

      default:
        // VARCHAR, VARSTRING, STRING, DECIMAL, OLDDECIMAL
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return parseBigDecimal(str, factory);
        } catch (NumberFormatException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as BigDecimal", str));
        }
    }
  }

  @Override
  public void encodeDirectText(ByteBuf out, Object value, Context context) {
    out.writeCharSequence(((BigDecimal) value).toPlainString(), StandardCharsets.US_ASCII);
  }

  @Override
  public void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    String v = ((BigDecimal) value).toPlainString();
    out.writeBytes(BufferUtils.encodeLength(v.length()));
    out.writeCharSequence(v, StandardCharsets.US_ASCII);
  }

  public DataType getBinaryEncodeType() {
    return DataType.DECIMAL;
  }
}

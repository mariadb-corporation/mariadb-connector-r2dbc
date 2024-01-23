// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

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
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.UTF_8).toString());

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
          return new BigDecimal(str);
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
          return new BigDecimal(str);
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

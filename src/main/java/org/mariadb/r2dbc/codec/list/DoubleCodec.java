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
          DataType.TEXT,
          DataType.VARSTRING,
          DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Double.TYPE) || type.isAssignableFrom(Double.class));
  }

  public boolean canEncode(Class<?> value) {
    return Double.class.isAssignableFrom(value);
  }

  @Override
  public Double decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Double> type,
      ExceptionFactory factory) {
    switch (column.getDataType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case DECIMAL:
      case YEAR:
        return Double.valueOf(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());

      default:
        // VARCHAR, VARSTRING, STRING:
        String str2 = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return Double.valueOf(str2);
        } catch (NumberFormatException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as Double", str2));
        }
    }
  }

  @Override
  public Double decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Double> type,
      ExceptionFactory factory) {
    switch (column.getDataType()) {
      case DOUBLE:
        return buf.readDoubleLE();

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

      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString())
            .doubleValue();

      default:
        // VARCHAR, VARSTRING, STRING:
        String str2 = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return Double.valueOf(str2);
        } catch (NumberFormatException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as Double", str2));
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
    out.writeDoubleLE((Double) value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.DOUBLE;
  }
}

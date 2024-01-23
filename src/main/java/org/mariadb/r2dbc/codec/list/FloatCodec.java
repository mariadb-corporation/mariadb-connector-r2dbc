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

public class FloatCodec implements Codec<Float> {

  public static final FloatCodec INSTANCE = new FloatCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.BIGINT,
          DataType.OLDDECIMAL,
          DataType.DECIMAL,
          DataType.YEAR,
          DataType.DOUBLE,
          DataType.TEXT,
          DataType.VARSTRING,
          DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Float.TYPE) || type.isAssignableFrom(Float.class));
  }

  public boolean canEncode(Class<?> value) {
    return Float.class.isAssignableFrom(value);
  }

  @Override
  public Float decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Float> type,
      ExceptionFactory factory) {
    switch (column.getDataType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case DOUBLE:
      case OLDDECIMAL:
      case DECIMAL:
      case YEAR:
      case FLOAT:
        return Float.valueOf(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());

      default:
        // VARCHAR, VARSTRING, STRING:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return Float.valueOf(val);
        } catch (NumberFormatException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as Float", val));
        }
    }
  }

  @Override
  public Float decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Float> type,
      ExceptionFactory factory) {

    switch (column.getDataType()) {
      case FLOAT:
        return buf.readFloatLE();

      case TINYINT:
        if (!column.isSigned()) {
          return (float) buf.readUnsignedByte();
        }
        return (float) buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return (float) buf.readUnsignedShortLE();
        }
        return (float) buf.readShortLE();

      case MEDIUMINT:
        float v = column.isSigned() ? buf.readMediumLE() : buf.readUnsignedMediumLE();
        buf.readByte(); // needed since binary protocol exchange for medium are on 4 bytes
        return v;

      case INTEGER:
        if (!column.isSigned()) {
          return (float) buf.readUnsignedIntLE();
        }
        return (float) buf.readIntLE();

      case BIGINT:
        if (column.isSigned()) {
          return (float) buf.readLongLE();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          return new BigInteger(1, bb).floatValue();
        }

      case DOUBLE:
        return (float) buf.readDoubleLE();

      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString())
            .floatValue();

      default:
        String str2 = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          return Float.valueOf(str2);
        } catch (NumberFormatException nfe) {
          throw factory.createParsingException(
              String.format("value '%s' cannot be decoded as Float", str2));
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
    out.writeFloatLE((Float) value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.FLOAT;
  }
}

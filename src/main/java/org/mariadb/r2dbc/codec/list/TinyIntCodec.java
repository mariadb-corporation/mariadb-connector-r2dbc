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
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class TinyIntCodec implements Codec<Byte> {

  public static final TinyIntCodec INSTANCE = new TinyIntCodec();

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getType() == DataType.TINYINT
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Object value) {
    return false;
  }

  public String className() {
    return Byte.class.getName();
  }

  @Override
  public Byte decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Byte> type) {
    long result;
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case YEAR:
      case BIGINT:
        result = LongCodec.parse(buf, length);
        break;

      case BIT:
        result = 0;
        for (int i = 0; i < Math.min(length, 8); i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        if (length > 8) {
          buf.skipBytes(length - 8);
        }
        break;

      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case VARCHAR:
      case DECIMAL:
      case ENUM:
      case VARSTRING:
      case STRING:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
          break;
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as byte", str));
        }

      default:
        buf.skipBytes(length);
        throw new R2dbcNonTransientResourceException(
            String.format("Data type %s cannot be decoded as byte", column.getType()));
    }

    if ((short) result != result || (result < 0 && !column.isSigned())) {
      throw new R2dbcNonTransientResourceException("byte overflow");
    }

    return (byte) result;
  }

  @Override
  public Byte decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Byte> type) {
    long result;
    switch (column.getType()) {
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

      case INTEGER:
        result = column.isSigned() ? buf.readIntLE() : buf.readUnsignedIntLE();
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
            result = val.intValueExact();
            break;
          } catch (ArithmeticException ae) {
            throw new R2dbcNonTransientResourceException(
                String.format("value '%s' cannot be decoded as Byte", val.toString()));
          }
        }

      case BIT:
        result = 0;
        for (int i = 0; i < Math.min(length, 8); i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        if (length > 8) {
          buf.skipBytes(length - 8);
        }
        break;

      case FLOAT:
        result = (long) buf.readFloatLE();
        break;

      case DOUBLE:
        result = (long) buf.readDoubleLE();
        break;

      case OLDDECIMAL:
      case VARCHAR:
      case DECIMAL:
      case ENUM:
      case VARSTRING:
      case STRING:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
          break;
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' cannot be decoded as Byte", str));
        }

      default:
        buf.skipBytes(length);
        throw new R2dbcNonTransientResourceException(
            String.format("Data type %s cannot be decoded as Byte", column.getType()));
    }

    if ((byte) result != result || (result < 0 && !column.isSigned())) {
      throw new R2dbcNonTransientResourceException("Byte overflow");
    }

    return (byte) result;
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Byte value) {
    throw new R2dbcNonTransientResourceException("Unexpected error, Byte encoder expected");
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Byte value) {
    throw new R2dbcNonTransientResourceException("Unexpected error, Byte encoder expected");
  }

  public DataType getBinaryEncodeType() {
    return DataType.TINYINT;
  }

  @Override
  public String toString() {
    return "TinyIntCodec{}";
  }
}

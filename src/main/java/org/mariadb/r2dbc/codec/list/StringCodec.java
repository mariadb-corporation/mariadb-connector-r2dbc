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
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class StringCodec implements Codec<String> {

  public static final StringCodec INSTANCE = new StringCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.OLDDECIMAL,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.TIMESTAMP,
          DataType.BIGINT,
          DataType.MEDIUMINT,
          DataType.DATE,
          DataType.TIME,
          DataType.DATETIME,
          DataType.YEAR,
          DataType.NEWDATE,
          DataType.JSON,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.SET,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING);

  public static String zeroFillingIfNeeded(String value, ColumnDefinitionPacket col) {
    if (col.isZeroFill()) {
      StringBuilder zeroAppendStr = new StringBuilder();
      long zeroToAdd = col.getDisplaySize() - value.length();
      while (zeroToAdd-- > 0) {
        zeroAppendStr.append("0");
      }
      return zeroAppendStr.append(value).toString();
    }
    return value;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType()) && type.isAssignableFrom(String.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof String;
  }

  @Override
  public String decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends String> type) {
    if (column.getDataType() == DataType.BIT) {

      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
      sb.append("b'");
      boolean firstByteNonZero = false;
      for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
        boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
        if (b) {
          sb.append('1');
          firstByteNonZero = true;
        } else if (firstByteNonZero) {
          sb.append('0');
        }
      }
      sb.append("'");
      return sb.toString();
    }

    String rawValue = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    if (column.isZeroFill()) {
      return zeroFillingIfNeeded(rawValue, column);
    }
    return rawValue;
  }

  @Override
  public String decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends String> type) {

    switch (column.getDataType()) {
      case BIT:
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
        sb.append("b'");
        boolean firstByteNonZero = false;
        for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
          boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
          if (b) {
            sb.append('1');
            firstByteNonZero = true;
          } else if (firstByteNonZero) {
            sb.append('0');
          }
        }
        sb.append("'");
        return sb.toString();

      case TINYINT:
        if (!column.isSigned()) {
          return String.valueOf(buf.readUnsignedByte());
        }
        return String.valueOf((int) buf.readByte());

      case YEAR:
        String s = String.valueOf(buf.readUnsignedShortLE());
        while (s.length() < column.getLength()) s = "0" + s;
        return s;

      case SMALLINT:
        if (!column.isSigned()) {
          return String.valueOf(buf.readUnsignedShortLE());
        }
        return String.valueOf(buf.readShortLE());

      case MEDIUMINT:
        if (!column.isSigned()) {
          return String.valueOf((buf.readUnsignedMediumLE()));
        }
        return String.valueOf(buf.readMediumLE());

      case INTEGER:
        if (!column.isSigned()) {
          return String.valueOf(buf.readUnsignedIntLE());
        }
        return String.valueOf(buf.readIntLE());

      case BIGINT:
        BigInteger val;
        if (column.isSigned()) {
          val = BigInteger.valueOf(buf.readLongLE());
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int ii = 7; ii >= 0; ii--) {
            bb[ii] = buf.readByte();
          }
          val = new BigInteger(1, bb);
        }

        return new BigDecimal(String.valueOf(val)).setScale(column.getDecimals()).toPlainString();

      case FLOAT:
        return String.valueOf(buf.readFloatLE());

      case DOUBLE:
        return String.valueOf(buf.readDoubleLE());

      case TIME:
        long tDays = 0;
        int tHours = 0;
        int tMinutes = 0;
        int tSeconds = 0;
        long tMicroseconds = 0;
        boolean negate = false;

        if (length > 0) {
          negate = buf.readByte() == 0x01;
          if (length > 4) {
            tDays = buf.readUnsignedIntLE();
            if (length > 7) {
              tHours = buf.readByte();
              tMinutes = buf.readByte();
              tSeconds = buf.readByte();
              if (length > 8) {
                tMicroseconds = buf.readIntLE();
              }
            }
          }
        }

        Duration duration =
            Duration.ZERO
                .plusDays(tDays)
                .plusHours(tHours)
                .plusMinutes(tMinutes)
                .plusSeconds(tSeconds)
                .plusNanos(tMicroseconds * 1000);
        if (negate) return duration.negated().toString();
        return duration.toString();

      case DATE:
        int dateYear = buf.readUnsignedShortLE();
        int dateMonth = buf.readByte();
        int dateDay = buf.readByte();
        if (length > 4) {
          buf.skipBytes(length - 4);
        }
        return LocalDate.of(dateYear, dateMonth, dateDay).toString();

      case DATETIME:
      case TIMESTAMP:
        int year = buf.readUnsignedShortLE();
        int month = buf.readByte();
        int day = buf.readByte();
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        long microseconds = 0;

        if (length > 4) {
          hour = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();

          if (length > 7) {
            microseconds = buf.readUnsignedIntLE();
          }
        }
        return LocalDateTime.of(year, month, day, hour, minutes, seconds)
            .plusNanos(microseconds * 1000)
            .toString();

      default:
        return buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    }
  }

  @Override
  public void encodeText(ByteBuf buf, ConnectionContext context, String value) {
    BufferUtils.write(buf, value, true, true, context);
  }

  @Override
  public void encodeBinary(ByteBuf buf, ConnectionContext context, String value) {
    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
    BufferUtils.writeLengthEncode(valueBytes.length, buf);
    buf.writeBytes(valueBytes);
  }

  public DataType getBinaryEncodeType() {
    return DataType.VARCHAR;
  }

  @Override
  public String toString() {
    return "StringCodec{}";
  }
}

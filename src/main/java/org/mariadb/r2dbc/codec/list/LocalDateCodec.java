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
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class LocalDateCodec implements Codec<LocalDate> {

  public static final LocalDateCodec INSTANCE = new LocalDateCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATE,
          DataType.NEWDATE,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.YEAR,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING);

  public static int[] parseDate(ByteBuf buf, int length) {
    int[] datePart = new int[] {0, 0, 0};
    int partIdx = 0;
    int idx = 0;

    while (idx++ < length) {
      byte b = buf.readByte();
      if (b == '-') {
        partIdx++;
        continue;
      }
      datePart[partIdx] = datePart[partIdx] * 10 + b - 48;
    }

    if (datePart[0] == 0 && datePart[1] == 0 && datePart[2] == 0) {
      return null;
    }
    return datePart;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(LocalDate.class);
  }

  public boolean canEncode(Class<?> value) {
    return LocalDate.class.isAssignableFrom(value);
  }

  @Override
  public LocalDate decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends LocalDate> type) {

    int[] parts;
    switch (column.getType()) {
      case YEAR:
        short y = (short) LongCodec.parse(buf, length);

        if (length == 2 && column.getLength() == 2) {
          // YEAR(2) - deprecated
          if (y <= 69) {
            y += 2000;
          } else {
            y += 1900;
          }
        }

        return LocalDate.of(y, 1, 1);
      case NEWDATE:
      case DATE:
        parts = parseDate(buf, length);
        break;

      case TIMESTAMP:
      case DATETIME:
        parts =
            LocalDateTimeCodec.parseTimestamp(
                buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());
        break;

      default:
        // VARSTRING, VARCHAR, STRING:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        String[] stDatePart = val.split("-| ");
        if (stDatePart.length < 3) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        try {
          int year = Integer.valueOf(stDatePart[0]);
          int month = Integer.valueOf(stDatePart[1]);
          int dayOfMonth = Integer.valueOf(stDatePart[2]);
          return LocalDate.of(year, month, dayOfMonth);
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }
    }
    if (parts == null) return null;
    return LocalDate.of(parts[0], parts[1], parts[2]);
  }

  @Override
  public LocalDate decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends LocalDate> type) {

    int year;
    int month = 1;
    int dayOfMonth = 1;

    switch (column.getType()) {
      case TIMESTAMP:
      case DATETIME:
        year = buf.readUnsignedShortLE();
        month = buf.readByte();
        dayOfMonth = buf.readByte();

        if (length > 4) {
          buf.skipBytes(length - 4);
        }
        return LocalDate.of(year, month, dayOfMonth);

      case STRING:
      case VARCHAR:
      case VARSTRING:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        String[] stDatePart = val.split("-| ");
        if (stDatePart.length < 3) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        try {
          year = Integer.valueOf(stDatePart[0]);
          month = Integer.valueOf(stDatePart[1]);
          dayOfMonth = Integer.valueOf(stDatePart[2]);
          return LocalDate.of(year, month, dayOfMonth);
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

      default:
        // DATE, YEAR:
        year = buf.readUnsignedShortLE();

        if (column.getLength() == 2) {
          // YEAR(2) - deprecated
          if (year <= 69) {
            year += 2000;
          } else {
            year += 1900;
          }
        }

        if (length >= 4) {
          month = buf.readByte();
          dayOfMonth = buf.readByte();
        }
        return LocalDate.of(year, month, dayOfMonth);
    }
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, LocalDate value) {
    buf.writeByte('\'');
    buf.writeCharSequence(
        value.format(DateTimeFormatter.ISO_LOCAL_DATE), StandardCharsets.US_ASCII);
    buf.writeByte('\'');
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, LocalDate value) {
    buf.writeByte(7); // length
    buf.writeShortLE((short) value.get(ChronoField.YEAR));
    buf.writeByte(value.get(ChronoField.MONTH_OF_YEAR));
    buf.writeByte(value.get(ChronoField.DAY_OF_MONTH));
    buf.writeBytes(new byte[] {0, 0, 0});
  }

  public DataType getBinaryEncodeType() {
    return DataType.DATE;
  }
}

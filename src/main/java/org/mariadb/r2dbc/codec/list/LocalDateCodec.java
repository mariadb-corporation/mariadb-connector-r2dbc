// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

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
          DataType.TEXT,
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
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(LocalDate.class);
  }

  public boolean canEncode(Class<?> value) {
    return LocalDate.class.isAssignableFrom(value);
  }

  @Override
  public LocalDate decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends LocalDate> type) {

    int[] parts;
    switch (column.getDataType()) {
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
              String.format(
                  "value '%s' (%s) cannot be decoded as Date", val, column.getDataType()));
        }

        try {
          int year = Integer.valueOf(stDatePart[0]);
          int month = Integer.valueOf(stDatePart[1]);
          int dayOfMonth = Integer.valueOf(stDatePart[2]);
          return LocalDate.of(year, month, dayOfMonth);
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format(
                  "value '%s' (%s) cannot be decoded as Date", val, column.getDataType()));
        }
    }
    if (parts == null) return null;
    return LocalDate.of(parts[0], parts[1], parts[2]);
  }

  @Override
  public LocalDate decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends LocalDate> type) {

    int year = 0;
    int month = 1;
    int dayOfMonth = 1;

    switch (column.getDataType()) {
      case TIMESTAMP:
      case DATETIME:
        if (length > 0) {
          year = buf.readUnsignedShortLE();
          month = buf.readByte();
          dayOfMonth = buf.readByte();

          if (length > 4) {
            buf.skipBytes(length - 4);
          }
          return LocalDate.of(year, month, dayOfMonth);
        }
        return null;

      case YEAR:
        if (length > 0) {
          year = buf.readUnsignedShortLE();
          if (column.getLength() == 2) {
            // YEAR(2) - deprecated
            if (year <= 69) {
              year += 2000;
            } else {
              year += 1900;
            }
          }
        }
        return LocalDate.of(year, month, dayOfMonth);

      case DATE:
        if (length > 0) {
          year = buf.readUnsignedShortLE();
          month = buf.readByte();
          dayOfMonth = buf.readByte();
        }
        return LocalDate.of(year, month, dayOfMonth);

      default:
        // VARCHAR,VARSTRING,STRING:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        String[] stDatePart = val.split("-| ");
        if (stDatePart.length < 3) {
          throw new R2dbcNonTransientResourceException(
              String.format(
                  "value '%s' (%s) cannot be decoded as Date", val, column.getDataType()));
        }

        try {
          year = Integer.valueOf(stDatePart[0]);
          month = Integer.valueOf(stDatePart[1]);
          dayOfMonth = Integer.valueOf(stDatePart[2]);
          return LocalDate.of(year, month, dayOfMonth);
        } catch (NumberFormatException nfe) {
          throw new R2dbcNonTransientResourceException(
              String.format(
                  "value '%s' (%s) cannot be decoded as Date", val, column.getDataType()));
        }
    }
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Object value) {
    buf.writeByte('\'');
    buf.writeCharSequence(
        ((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE), StandardCharsets.US_ASCII);
    buf.writeByte('\'');
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Object val) {
    LocalDate value = (LocalDate) val;
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

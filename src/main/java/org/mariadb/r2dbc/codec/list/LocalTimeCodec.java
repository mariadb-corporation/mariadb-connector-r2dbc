// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class LocalTimeCodec implements Codec<LocalTime> {

  public static final LocalTimeCodec INSTANCE = new LocalTimeCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TIME,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARSTRING,
          DataType.TEXT,
          DataType.STRING);

  public static int[] parseTime(
      ByteBuf buf, int length, ColumnDefinitionPacket column, ExceptionFactory factory) {
    int initialPos = buf.readerIndex();
    int[] parts = new int[5];
    int idx = 1;
    int partLength = 0;
    byte b;
    int i = 0;
    if (length > 0 && buf.getByte(buf.readerIndex()) == '-') {
      buf.skipBytes(1);
      i++;
      parts[0] = 1;
    }

    for (; i < length; i++) {
      b = buf.readByte();
      if (b == ':' || b == '.') {
        idx++;
        partLength = 0;
        continue;
      }
      if (b < '0' || b > '9') {
        buf.readerIndex(initialPos);
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        throw factory.createParsingException(
            String.format("%s value '%s' cannot be decoded as Time", column.getDataType(), val));
      }
      partLength++;
      parts[idx] = parts[idx] * 10 + (b - '0');
    }

    if (idx < 2) {
      buf.readerIndex(initialPos);
      String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
      throw factory.createParsingException(
          String.format("%s value '%s' cannot be decoded as Time", column.getDataType(), val));
    }

    // set nano real value
    if (idx == 4) {
      for (i = 0; i < 9 - partLength; i++) {
        parts[4] = parts[4] * 10;
      }
    }
    return parts;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(LocalTime.class);
  }

  public boolean canEncode(Class<?> value) {
    return LocalTime.class.isAssignableFrom(value);
  }

  @Override
  public LocalTime decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends LocalTime> type,
      ExceptionFactory factory) {

    int[] parts;
    switch (column.getDataType()) {
      case TIMESTAMP:
      case DATETIME:
        parts =
            LocalDateTimeCodec.parseTimestamp(
                buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());
        if (parts == null) return null;
        return LocalTime.of(parts[3], parts[4], parts[5], parts[6]);

      case TIME:
        parts = parseTime(buf, length, column, factory);
        parts[1] = parts[1] % 24;
        if (parts[0] == 1) {
          // negative

          long seconds = (24 * 60 * 60 - (parts[1] * 3600 + parts[2] * 60L + parts[3]));
          return LocalTime.ofNanoOfDay(seconds * 1_000_000_000 - parts[4]);
        }
        return LocalTime.of(parts[1] % 24, parts[2], parts[3], parts[4]);

      default:
        // STRING, VARCHAR, VARSTRING:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          if (val.contains(" ")) {
            return LocalDateTime.parse(val, LocalDateTimeCodec.MARIADB_LOCAL_DATE_TIME)
                .toLocalTime();
          } else {
            return LocalTime.parse(val);
          }
        } catch (DateTimeParseException e) {
          throw factory.createParsingException(
              String.format(
                  "value '%s' (%s) cannot be decoded as LocalTime", val, column.getDataType()));
        }
    }
  }

  @Override
  public LocalTime decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends LocalTime> type,
      ExceptionFactory factory) {

    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;
    switch (column.getDataType()) {
      case TIMESTAMP:
      case DATETIME:
        if (length > 0) {
          buf.skipBytes(4); // skip year, month and day
          if (length > 4) {
            hour = buf.readByte();
            minutes = buf.readByte();
            seconds = buf.readByte();

            if (length > 7) {
              microseconds = buf.readIntLE();
            }
          }
          return LocalTime.of(hour, minutes, seconds).plusNanos(microseconds * 1000);
        }
        return null;

      case TIME:
        boolean negate = false;
        if (length > 0) {
          negate = buf.readByte() == 1;
          buf.skipBytes(4); // skip days
          hour = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();
          if (length > 8) {
            microseconds = buf.readIntLE();
          }
        }
        if (negate) {
          // negative
          long nanos = (24 * 60 * 60 - (hour * 3600 + minutes * 60 + seconds));
          return LocalTime.ofNanoOfDay(nanos * 1_000_000_000 - microseconds * 1000);
        }
        return LocalTime.of(hour % 24, minutes, seconds, (int) microseconds * 1000);

      default:
        // STRING, VARCHAR, VARSTRING:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          if (val.contains(" ")) {
            return LocalDateTime.parse(val, LocalDateTimeCodec.MARIADB_LOCAL_DATE_TIME)
                .toLocalTime();
          } else {
            return LocalTime.parse(val);
          }
        } catch (DateTimeParseException e) {
          throw factory.createParsingException(
              String.format(
                  "value '%s' (%s) cannot be decoded as LocalTime", val, column.getDataType()));
        }
    }
  }

  @Override
  public void encodeDirectText(ByteBuf out, Object value, Context context) {
    LocalTime val = (LocalTime) value;
    StringBuilder dateString = new StringBuilder(15);
    dateString
        .append(val.getHour() < 10 ? "0" : "")
        .append(val.getHour())
        .append(val.getMinute() < 10 ? ":0" : ":")
        .append(val.getMinute())
        .append(val.getSecond() < 10 ? ":0" : ":")
        .append(val.getSecond());

    int microseconds = val.getNano() / 1000;
    if (microseconds > 0) {
      dateString.append(".");
      if (microseconds % 1000 == 0) {
        dateString.append(Integer.toString(microseconds / 1000 + 1000).substring(1));
      } else {
        dateString.append(Integer.toString(microseconds + 1000000).substring(1));
      }
    }

    out.writeByte('\'');
    out.writeCharSequence(dateString.toString(), StandardCharsets.US_ASCII);
    out.writeByte('\'');
  }

  @Override
  public void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    LocalTime val = (LocalTime) value;
    int nano = val.getNano();
    if (nano > 0) {
      out.writeByte((byte) 12);
      out.writeByte((byte) 0);
      out.writeIntLE(0);
      out.writeByte((byte) val.getHour());
      out.writeByte((byte) val.getMinute());
      out.writeByte((byte) val.getSecond());
      out.writeIntLE(nano / 1000);
    } else {
      out.writeByte((byte) 8);
      out.writeByte((byte) 0);
      out.writeIntLE(0);
      out.writeByte((byte) val.getHour());
      out.writeByte((byte) val.getMinute());
      out.writeByte((byte) val.getSecond());
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.TIME;
  }
}

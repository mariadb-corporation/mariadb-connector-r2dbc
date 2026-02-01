// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class InstantCodec implements Codec<Instant> {

  public static final InstantCodec INSTANCE = new InstantCodec();
  public static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  public static final DateTimeFormatter TIMESTAMP_FORMAT_NO_FRACTIONAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARSTRING,
          DataType.TEXT,
          DataType.STRING,
          DataType.TIME,
          DataType.DATE);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType()) && type.isAssignableFrom(Instant.class);
  }

  public boolean canEncode(Class<?> value) {
    return Instant.class.isAssignableFrom(value);
  }

  @Override
  public Instant decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Instant> type,
      ExceptionFactory factory) {

    int[] parts;
    switch (column.getDataType()) {
      case DATE:
        parts = LocalDateCodec.parseDate(buf, length);
        if (parts == null) return null;
        return LocalDateTime.of(parts[0], parts[1], parts[2], 0, 0, 0)
            .atZone(ZoneId.systemDefault())
            .toInstant();

      case DATETIME:
      case TIMESTAMP:
        parts =
            LocalDateTimeCodec.parseTimestamp(
                buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());
        if (parts == null) return null;
        return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
            .plusNanos(parts[6])
            .atZone(ZoneId.systemDefault())
            .toInstant();

      case TIME:
        parts = LocalTimeCodec.parseTime(buf, length, column, factory);
        return LocalDateTime.of(1970, 1, 1, parts[1] % 24, parts[2], parts[3])
            .plusNanos(parts[4])
            .atZone(ZoneId.systemDefault())
            .toInstant();

      default:
        // STRING, VARCHAR, VARSTRING:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          parts = LocalDateTimeCodec.parseTimestamp(val);
          if (parts == null) return null;
          return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
              .plusNanos(parts[6])
              .atZone(ZoneId.systemDefault())
              .toInstant();
        } catch (DateTimeException dte) {
          throw factory.createParsingException(
              String.format(
                  "value '%s' (%s) cannot be decoded as LocalDateTime", val, column.getDataType()));
        }
    }
  }

  @Override
  public Instant decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Instant> type,
      ExceptionFactory factory) {

    int year = 1970;
    int month = 1;
    long dayOfMonth = 1;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    switch (column.getDataType()) {
      case TIME:
        // specific case for TIME, to handle value not in 00:00:00-23:59:59
        if (length > 0) {
          buf.skipBytes(5); // skip negative and days
          hour = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();
          if (length > 8) {
            microseconds = buf.readUnsignedIntLE();
          }
        }
        break;

      case DATE:
      case TIMESTAMP:
      case DATETIME:
        if (length > 0) {
          year = buf.readUnsignedShortLE();
          month = buf.readByte();
          dayOfMonth = buf.readByte();

          if (length > 4) {
            hour = buf.readByte();
            minutes = buf.readByte();
            seconds = buf.readByte();

            if (length > 7) {
              microseconds = buf.readUnsignedIntLE();
            }
          }
          if (year == 0
              && month == 0
              && dayOfMonth == 0
              && hour == 0
              && minutes == 0
              && seconds == 0) return null;
        } else return null;
        break;

      default:
        // STRING, VARCHAR, VARSTRING:
        String val = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          int[] parts = LocalDateTimeCodec.parseTimestamp(val);
          if (parts == null) return null;
          return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
              .plusNanos(parts[6])
              .atZone(ZoneId.systemDefault())
              .toInstant();
        } catch (DateTimeException dte) {
          throw factory.createParsingException(
              String.format(
                  "value '%s' (%s) cannot be decoded as LocalDateTime", val, column.getDataType()));
        }
    }

    return LocalDateTime.of(year, month, (int) dayOfMonth, hour, minutes, seconds)
        .plusNanos(microseconds * 1000)
        .atZone(ZoneId.systemDefault())
        .toInstant();
  }

  @Override
  public void encodeDirectText(ByteBuf out, Object value, Context context) {
    Instant val = (Instant) value;
    LocalDateTime ldt = LocalDateTime.ofInstant(val, ZoneId.systemDefault());
    out.writeByte('\'');
    out.writeCharSequence(
        ldt.format(
            ldt.getNano() != 0
                ? LocalDateTimeCodec.TIMESTAMP_FORMAT
                : LocalDateTimeCodec.TIMESTAMP_FORMAT_NO_FRACTIONAL),
        StandardCharsets.US_ASCII);
    out.writeByte('\'');
  }

  @Override
  public void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    Instant val = (Instant) value;
    LocalDateTime ldt = LocalDateTime.ofInstant(val, ZoneId.systemDefault());
    int nano = ldt.getNano();
    if (nano > 0) {
      out.writeByte((byte) 11);
      out.writeShortLE((short) ldt.getYear());
      out.writeByte(ldt.getMonthValue());
      out.writeByte(ldt.getDayOfMonth());
      out.writeByte(ldt.getHour());
      out.writeByte(ldt.getMinute());
      out.writeByte(ldt.getSecond());
      out.writeIntLE(nano / 1000);
    } else {
      out.writeByte((byte) 7);
      out.writeShortLE((short) ldt.getYear());
      out.writeByte(ldt.getMonthValue());
      out.writeByte(ldt.getDayOfMonth());
      out.writeByte(ldt.getHour());
      out.writeByte(ldt.getMinute());
      out.writeByte(ldt.getSecond());
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.DATETIME;
  }
}

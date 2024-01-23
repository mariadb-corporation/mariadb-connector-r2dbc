// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class DurationCodec implements Codec<Duration> {

  public static final DurationCodec INSTANCE = new DurationCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TIME,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARSTRING,
          DataType.TEXT,
          DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType()) && type.isAssignableFrom(Duration.class);
  }

  public boolean canEncode(Class<?> value) {
    return Duration.class.isAssignableFrom(value);
  }

  @Override
  public Duration decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Duration> type,
      ExceptionFactory factory) {

    int[] parts;
    switch (column.getDataType()) {
      case TIMESTAMP:
      case DATETIME:
        parts =
            LocalDateTimeCodec.parseTimestamp(
                buf.readCharSequence(length, StandardCharsets.US_ASCII).toString());
        if (parts == null) return null;
        return Duration.ZERO
            .plusDays(parts[2] - 1)
            .plusHours(parts[3])
            .plusMinutes(parts[4])
            .plusSeconds(parts[5])
            .plusNanos(parts[6]);

      default:
        // TIME, VARCHAR, VARSTRING, STRING:
        parts = LocalTimeCodec.parseTime(buf, length, column, factory);
        Duration d =
            Duration.ZERO
                .plusHours(parts[1])
                .plusMinutes(parts[2])
                .plusSeconds(parts[3])
                .plusNanos(parts[4]);
        if (parts[0] == 1) return d.negated();
        return d;
    }
  }

  @Override
  public Duration decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Duration> type,
      ExceptionFactory factory) {

    long days = 0;
    int hours = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    switch (column.getDataType()) {
      case TIME:
        boolean negate = false;
        if (length > 0) {
          negate = buf.readUnsignedByte() == 0x01;
          days = buf.readUnsignedIntLE();
          hours = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();
          if (length > 8) {
            microseconds = buf.readIntLE();
          }
        }

        Duration duration =
            Duration.ZERO
                .plusDays(days)
                .plusHours(hours)
                .plusMinutes(minutes)
                .plusSeconds(seconds)
                .plusNanos(microseconds * 1000);
        if (negate) return duration.negated();
        return duration;

      case TIMESTAMP:
      case DATETIME:
        if (length > 0) {
          buf.readUnsignedShortLE(); // skip year
          buf.readByte(); // skip month
          days = buf.readByte();
          if (length > 4) {
            hours = buf.readByte();
            minutes = buf.readByte();
            seconds = buf.readByte();
            if (length > 7) {
              microseconds = buf.readUnsignedIntLE();
            }
          }
        }
        return Duration.ZERO
            .plusDays(days - 1)
            .plusHours(hours)
            .plusMinutes(minutes)
            .plusSeconds(seconds)
            .plusNanos(microseconds * 1000);

      default:
        // VARCHAR, VARSTRING, STRING:
        int[] parts = LocalTimeCodec.parseTime(buf, length, column, factory);
        Duration d =
            Duration.ZERO
                .plusHours(parts[1])
                .plusMinutes(parts[2])
                .plusSeconds(parts[3])
                .plusNanos(parts[4]);
        if (parts[0] == 1) return d.negated();
        return d;
    }
  }

  @Override
  public void encodeDirectText(ByteBuf out, Object value, Context context) {

    Duration val = (Duration) value;
    long s = val.getSeconds();
    boolean negate = false;
    if (s < 0) {
      negate = true;
      s = -s;
    }

    long microSecond = val.getNano() / 1000;
    out.writeByte('\'');
    String durationStr;
    if (microSecond != 0) {
      if (negate) {
        s = s - 1;
        durationStr =
            String.format(
                "-%d:%02d:%02d.%06d", s / 3600, (s % 3600) / 60, (s % 60), 1000000 - microSecond);

      } else {
        durationStr =
            String.format("%d:%02d:%02d.%06d", s / 3600, (s % 3600) / 60, (s % 60), microSecond);
      }
    } else {
      durationStr =
          String.format(
              negate ? "-%d:%02d:%02d" : "%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
    }
    out.writeCharSequence(durationStr, StandardCharsets.US_ASCII);
    out.writeByte('\'');
  }

  @Override
  public void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    Duration val = (Duration) value;

    long microSecond = val.getNano() / 1000;
    long s = Math.abs(val.getSeconds());
    if (microSecond > 0) {
      if (val.isNegative()) {
        s = s - 1;
        out.writeByte((byte) 12);
        out.writeByte((byte) (val.isNegative() ? 1 : 0));
        out.writeIntLE((int) (s / (24 * 3600)));
        out.writeByte((int) (s % (24 * 3600)) / 3600);
        out.writeByte((int) (s % 3600) / 60);
        out.writeByte((int) (s % 60));
        out.writeIntLE((int) (1000000 - microSecond));
      } else {
        out.writeByte((byte) 12);
        out.writeByte((byte) (val.isNegative() ? 1 : 0));
        out.writeIntLE((int) (s / (24 * 3600)));
        out.writeByte((int) (s % (24 * 3600)) / 3600);
        out.writeByte((int) (s % 3600) / 60);
        out.writeByte((int) (s % 60));
        out.writeIntLE((int) microSecond);
      }
    } else {
      out.writeByte((byte) 8);
      out.writeByte((byte) (val.isNegative() ? 1 : 0));
      out.writeIntLE((int) (s / (24 * 3600)));
      out.writeByte((int) (s % (24 * 3600)) / 3600);
      out.writeByte((int) (s % 3600) / 60);
      out.writeByte((int) (s % 60));
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.TIME;
  }
}

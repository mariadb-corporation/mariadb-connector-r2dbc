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
import java.time.Duration;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class DurationCodec implements Codec<Duration> {

  public static final DurationCodec INSTANCE = new DurationCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TIME,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING);

  public String className() {
    return Duration.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Duration.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Duration;
  }

  @Override
  public Duration decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Duration> type) {

    int[] parts;
    switch (column.getType()) {
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

      case TIME:
      case VARCHAR:
      case VARSTRING:
      case STRING:
        parts = LocalTimeCodec.parseTime(buf, length, column);
        Duration d =
            Duration.ZERO
                .plusHours(parts[1])
                .plusMinutes(parts[2])
                .plusSeconds(parts[3])
                .plusNanos(parts[4]);
        if (parts[0] == 1) return d.negated();
        return d;

      default:
        buf.skipBytes(length);
        throw new R2dbcNonTransientResourceException(
            String.format("Data type %s cannot be decoded as Duration", column.getType()));
    }
  }

  @Override
  public Duration decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Duration> type) {

    long days = 0;
    int hours = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    switch (column.getType()) {
      case TIME:
        boolean negate = false;
        if (length > 0) {
          negate = buf.readUnsignedByte() == 0x01;
          if (length > 4) {
            days = buf.readUnsignedIntLE();
            if (length > 7) {
              hours = buf.readByte();
              minutes = buf.readByte();
              seconds = buf.readByte();
              if (length > 8) {
                microseconds = buf.readIntLE();
              }
            }
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

      default:
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
        return Duration.ZERO
            .plusDays(days - 1)
            .plusHours(hours)
            .plusMinutes(minutes)
            .plusSeconds(seconds)
            .plusNanos(microseconds * 1000);
    }
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Duration val) {
    long s = val.getSeconds();
    long microSecond = val.getNano() / 1000;
    buf.writeByte('\'');
    if (microSecond != 0) {
      buf.writeCharSequence(
          String.format("%d:%02d:%02d.%06d", s / 3600, (s % 3600) / 60, (s % 60), microSecond),
          StandardCharsets.US_ASCII);
    } else {
      buf.writeCharSequence(
          String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60)),
          StandardCharsets.US_ASCII);
    }
    buf.writeByte('\'');
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Duration value) {
    int nano = value.getNano();
    if (nano > 0) {
      buf.writeByte((byte) 12);
      buf.writeByte((byte) (value.isNegative() ? 1 : 0));
      buf.writeIntLE((int) value.toDays());
      buf.writeByte((byte) (value.toHours() - 24 * value.toDays()));
      buf.writeByte((byte) (value.toMinutes() - 60 * value.toHours()));
      buf.writeByte((byte) (value.getSeconds() - 60 * value.toMinutes()));
      buf.writeIntLE(nano / 1000);
    } else {
      buf.writeByte((byte) 8);
      buf.writeByte((byte) (value.isNegative() ? 1 : 0));
      buf.writeIntLE((int) value.toDays());
      buf.writeByte((byte) (value.toHours() - 24 * value.toDays()));
      buf.writeByte((byte) (value.toMinutes() - 60 * value.toHours()));
      buf.writeByte((byte) (value.getSeconds() - 60 * value.toMinutes()));
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.TIME;
  }

  @Override
  public String toString() {
    return "DurationCodec{}";
  }
}

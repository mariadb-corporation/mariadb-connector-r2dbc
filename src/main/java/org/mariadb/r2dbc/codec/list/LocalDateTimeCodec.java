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
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class LocalDateTimeCodec implements Codec<LocalDateTime> {

  public static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.DATETIME, DataType.TIMESTAMP);

  public static int[] parseTimestamp(ByteBuf buf, int length) {
    int nanoLen = -1;
    int[] timestampsPart = new int[] {0, 0, 0, 0, 0, 0, 0};
    int partIdx = 0;
    int idx = 0;
    while (idx++ < length) {
      byte b = buf.readByte();
      if (b == '-' || b == ' ' || b == ':') {
        partIdx++;
        continue;
      }
      if (b == '.') {
        partIdx++;
        nanoLen = 0;
        continue;
      }
      if (nanoLen >= 0) nanoLen++;
      timestampsPart[partIdx] = timestampsPart[partIdx] * 10 + b - 48;
    }
    if (timestampsPart[0] == 0 && timestampsPart[1] == 0 && timestampsPart[2] == 0) {
      if (timestampsPart[3] == 0
          && timestampsPart[4] == 0
          && timestampsPart[5] == 0
          && timestampsPart[6] == 0) return null;
      timestampsPart[1] = 1;
      timestampsPart[2] = 1;
    }

    // fix non leading tray for nanoseconds
    if (nanoLen >= 0) {
      for (int begin = 0; begin < 6 - nanoLen; begin++) {
        timestampsPart[6] = timestampsPart[6] * 10;
      }
      timestampsPart[6] = timestampsPart[6] * 1000;
    }
    return timestampsPart;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(LocalDateTime.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof LocalDateTime;
  }

  @Override
  public LocalDateTime decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends LocalDateTime> type) {

    if (column.getDataType() == DataType.TIMESTAMP || column.getDataType() == DataType.DATETIME) {
      int[] parts = parseTimestamp(buf, length);
      if (parts == null) return null;
      return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
          .plusNanos(parts[6]);
    }
    buf.skipBytes(length);
    throw new IllegalArgumentException("date type not supported");
  }

  @Override
  public LocalDateTime decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends LocalDateTime> type) {

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
        .plusNanos(microseconds * 1000);
  }

  @Override
  public void encodeText(ByteBuf buf, ConnectionContext context, LocalDateTime value) {
    BufferUtils.write(buf, value);
  }

  @Override
  public void encodeBinary(ByteBuf buf, ConnectionContext context, LocalDateTime value) {

    int nano = value.getNano();
    if (nano > 0) {
      buf.writeByte((byte) 11);
      buf.writeShortLE(value.get(ChronoField.YEAR));
      buf.writeByte(value.get(ChronoField.MONTH_OF_YEAR));
      buf.writeByte(value.get(ChronoField.DAY_OF_MONTH));
      buf.writeByte(value.get(ChronoField.HOUR_OF_DAY));
      buf.writeByte(value.get(ChronoField.MINUTE_OF_HOUR));
      buf.writeByte(value.get(ChronoField.SECOND_OF_MINUTE));
      buf.writeIntLE(nano / 1000);
    } else {
      buf.writeByte((byte) 7);
      buf.writeShortLE(value.get(ChronoField.YEAR));
      buf.writeByte(value.get(ChronoField.MONTH_OF_YEAR));
      buf.writeByte(value.get(ChronoField.DAY_OF_MONTH));
      buf.writeByte(value.get(ChronoField.HOUR_OF_DAY));
      buf.writeByte(value.get(ChronoField.MINUTE_OF_HOUR));
      buf.writeByte(value.get(ChronoField.SECOND_OF_MINUTE));
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.DATETIME;
  }

  @Override
  public String toString() {
    return "LocalDateTimeCodec{}";
  }
}

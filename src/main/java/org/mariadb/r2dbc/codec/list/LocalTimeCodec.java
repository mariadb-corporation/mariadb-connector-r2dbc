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
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class LocalTimeCodec implements Codec<LocalTime> {

  public static final LocalTimeCodec INSTANCE = new LocalTimeCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.TIME, DataType.DATETIME, DataType.TIMESTAMP);

  public static int[] parseTime(ByteBuf buf, int length) {
    String raw = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    boolean negate = raw.startsWith("-");
    if (negate) {
      raw = raw.substring(1);
    }
    String[] rawPart = raw.split(":");
    if (rawPart.length == 3) {
      int hour = Integer.parseInt(rawPart[0]);
      int minutes = Integer.parseInt(rawPart[1]);
      int seconds = Integer.parseInt(rawPart[2].substring(0, 2));
      int nanoseconds = extractNanos(raw);

      return new int[] {hour, minutes, seconds, nanoseconds};

    } else {
      throw new IllegalArgumentException(
          String.format(
              "%s cannot be parse as time. time must have" + " \"99:99:99\" format", raw));
    }
  }

  protected static int extractNanos(String timestring) {
    int index = timestring.indexOf('.');
    if (index == -1) {
      return 0;
    }
    int nanos = 0;
    for (int i = index + 1; i < index + 10; i++) {
      int digit;
      if (i >= timestring.length()) {
        digit = 0;
      } else {
        char value = timestring.charAt(i);
        if (value < '0' || value > '9') {
          throw new IllegalArgumentException(
              String.format(
                  "cannot parse sub-second part in " + "timestamp string '%s'", timestring));
        }
        digit = value - '0';
      }
      nanos = nanos * 10 + digit;
    }
    return nanos;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(LocalTime.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof LocalTime;
  }

  @Override
  public LocalTime decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends LocalTime> type) {

    int[] parts;
    switch (column.getDataType()) {
      case TIME:
        parts = parseTime(buf, length);
        return LocalTime.of(parts[0] % 24, parts[1], parts[2], parts[3]);

      case TIMESTAMP:
      case DATETIME:
        parts = LocalDateTimeCodec.parseTimestamp(buf, length);
        if (parts == null) return null;
        return LocalTime.of(parts[3], parts[4], parts[5], parts[6]);

      default:
        buf.skipBytes(length);
        throw new IllegalArgumentException(
            String.format("type %s not supported", column.getDataType()));
    }
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, LocalTime value) {
    BufferUtils.write(buf, value);
  }

  @Override
  public String toString() {
    return "LocalTimeCodec{}";
  }
}

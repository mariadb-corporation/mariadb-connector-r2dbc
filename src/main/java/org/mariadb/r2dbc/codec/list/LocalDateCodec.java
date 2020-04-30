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
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class LocalDateCodec implements Codec<LocalDate> {

  public static final LocalDateCodec INSTANCE = new LocalDateCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.DATE, DataType.NEWDATE, DataType.DATETIME, DataType.TIMESTAMP);

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
      if (b < '0' || b > '9') {
        buf.skipBytes(length - idx);
        throw new IllegalArgumentException(String.format("Illegal date format: value %s", b));
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

  public boolean canEncode(Object value) {
    return value instanceof LocalDate;
  }

  @Override
  public LocalDate decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends LocalDate> type) {

    int[] parts;
    switch (column.getDataType()) {
      case NEWDATE:
      case DATE:
        parts = parseDate(buf, length);
        break;

      default:
        parts = LocalDateTimeCodec.parseTimestamp(buf, length);
        break;
    }
    if (parts == null) return null;
    return LocalDate.of(parts[0], parts[1], parts[2]);
  }

  @Override
  public LocalDate decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends LocalDate> type) {

    int year;
    int month = 1;
    int day = 1;

    switch (column.getDataType()) {
      case TIMESTAMP:
      case DATETIME:
        year = buf.readUnsignedShortLE();
        month = buf.readByte();
        day = buf.readByte();

        if (length > 4) {
          buf.skipBytes(length - 4);
        }
        return LocalDate.of(year, month, day);

      default:
        year = buf.readUnsignedShortLE();

        if (length == 2 && column.getLength() == 2) {
          // YEAR(2) - deprecated
          if (year <= 69) {
            year += 2000;
          } else {
            year += 1900;
          }
        }

        if (length >= 4) {
          month = buf.readByte();
          day = buf.readByte();
        }
        return LocalDate.of(year, month, day);
    }
  }

  @Override
  public void encodeText(ByteBuf buf, ConnectionContext context, LocalDate value) {
    BufferUtils.write(buf, value);
  }

  @Override
  public void encodeBinary(ByteBuf buf, ConnectionContext context, LocalDate value) {
    buf.writeByte(7); // length
    buf.writeShortLE((short) value.get(ChronoField.YEAR));
    buf.writeByte(value.get(ChronoField.MONTH_OF_YEAR));
    buf.writeByte(value.get(ChronoField.DAY_OF_MONTH));
    buf.writeBytes(new byte[] {0, 0, 0});
  }

  public DataType getBinaryEncodeType() {
    return DataType.DATE;
  }

  @Override
  public String toString() {
    return "LocalDateCodec{}";
  }
}

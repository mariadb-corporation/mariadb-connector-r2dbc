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
import java.util.BitSet;
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
      BitSet val = BitSetCodec.parseBit(buf, length);
      StringBuilder sb = new StringBuilder(val.length() * 8 + 3);
      sb.append("b'");
      int i = val.length();
      while (i-- > 0) {
        sb.append(val.get(i) ? "1" : "0");
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
  public void encode(ByteBuf buf, ConnectionContext context, String value) {
    BufferUtils.write(buf, value, true, true, context);
  }

  @Override
  public String toString() {
    return "StringCodec{}";
  }
}

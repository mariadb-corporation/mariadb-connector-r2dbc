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
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class FloatCodec implements Codec<Float> {

  public static final FloatCodec INSTANCE = new FloatCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.BIGINT,
          DataType.DECIMAL,
          DataType.YEAR);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Float.TYPE) || type.isAssignableFrom(Float.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Float;
  }

  @Override
  public Float decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Float> type) {
    if (column.getDataType() == DataType.BIT) {
      return Float.valueOf(ByteCodec.parseBit(buf, length));
    }
    String str = buf.readCharSequence(length, StandardCharsets.US_ASCII).toString();
    try {
      return Float.valueOf(str);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(String.format("Incorrect float format %s", str));
    }
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, Float value) {
    BufferUtils.writeAscii(buf, String.valueOf(value));
  }

  @Override
  public String toString() {
    return "FloatCodec{}";
  }
}

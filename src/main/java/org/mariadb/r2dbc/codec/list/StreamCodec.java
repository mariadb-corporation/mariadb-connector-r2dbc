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
import io.netty.buffer.ByteBufInputStream;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

import java.io.InputStream;
import java.util.EnumSet;

public class StreamCodec implements Codec<InputStream> {

  public static final StreamCodec INSTANCE = new StreamCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.BLOB, DataType.TINYBLOB, DataType.MEDIUMBLOB, DataType.LONGBLOB);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Byte.TYPE && type.isArray())
            || type.isAssignableFrom(byte[].class));
  }

  @Override
  public InputStream decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends InputStream> type) {
    return new ByteBufInputStream(buf.readSlice(length));
  }

  public boolean canEncode(Object value) {
    return value instanceof InputStream;
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, InputStream value) {
    BufferUtils.write(buf, value, context);
  }

  @Override
  public String toString() {
    return "StreamCodec{}";
  }
}

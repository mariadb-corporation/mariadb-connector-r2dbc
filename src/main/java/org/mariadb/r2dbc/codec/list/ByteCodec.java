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
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class ByteCodec implements Codec<Byte> {

  public static final ByteCodec INSTANCE = new ByteCodec();

  public static long parseBit(ByteBuf buf, int length) {
    if (length == 1) {
      return buf.readByte();
    }
    long val = 0;
    do {
      val += ((long) buf.readUnsignedByte()) << (8 * length);
    } while (length > 0);
    return val;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getDataType() == DataType.BIT
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  @Override
  public Byte decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Byte> type) {
    if (length == 0) {
      throw new IllegalArgumentException(
          String.format("Unexpected datatype %s", column.getDataType()));
    }
    Byte val = buf.readByte();
    if (length > 1) buf.skipBytes(length - 1);
    return val;
  }

  public boolean canEncode(Object value) {
    return value instanceof Byte;
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, Byte value) {
    BufferUtils.writeAscii(buf, Integer.toString((int) value));
  }

  @Override
  public String toString() {
    return "ByteCodec{}";
  }
}

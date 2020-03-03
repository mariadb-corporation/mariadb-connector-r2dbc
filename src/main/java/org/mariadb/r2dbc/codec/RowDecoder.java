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

package org.mariadb.r2dbc.codec;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public abstract class RowDecoder {
  protected static final int NULL_LENGTH = -1;

  public ByteBuf buf;
  protected int length;
  protected int index;

  public RowDecoder() {}

  public void resetRow(ByteBuf buf) {
    this.buf = buf;
    this.buf.markReaderIndex();
    index = -1;
  }

  public abstract void setPosition(int position);

  @SuppressWarnings("unchecked")
  public <T> T get(int index, ColumnDefinitionPacket column, Class<T> type) {
    setPosition(index);

    if (length == NULL_LENGTH) {
      if (type.isPrimitive()) {
        throw new IllegalArgumentException(
            String.format("Cannot return null for primitive %s", type.getName()));
      }
      return null;
    }

    // type generic, return "natural" java type
    if (Object.class == type || type == null) {
      Codec<T> defaultCodec = ((Codec<T>) column.getDefaultCodec());
      return defaultCodec.decodeText(buf, length, column, type);
    }

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canDecode(column, type)) {
        return ((Codec<T>) codec).decodeText(buf, length, column, type);
      }
    }

    if (type.isArray()) {
      throw new IllegalArgumentException(
          String.format(
              "No decoder for type %s[] and column type %s",
              type.getComponentType().getName(), column.getDataType().toString()));
    }
    throw new IllegalArgumentException(
        String.format(
            "No decoder for type %s and column type %s",
            type.getName(), column.getDataType().toString()));
  }
}

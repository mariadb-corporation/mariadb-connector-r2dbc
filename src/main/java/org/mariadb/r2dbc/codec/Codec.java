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
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public interface Codec<T> {

  boolean canDecode(ColumnDefinitionPacket column, Class<?> type);

  boolean canEncode(Object value);

  T decodeText(ByteBuf buffer, int length, ColumnDefinitionPacket column, Class<? extends T> type);

  void encodeText(ByteBuf buf, Context context, T value);

  T decodeBinary(
      ByteBuf buffer, int length, ColumnDefinitionPacket column, Class<? extends T> type);

  void encodeBinary(ByteBuf buf, Context context, T value);

  DataType getBinaryEncodeType();
}

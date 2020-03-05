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
import io.r2dbc.spi.Clob;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;
import reactor.core.publisher.Mono;

public class ClobCodec implements Codec<Clob> {

  public static final ClobCodec INSTANCE = new ClobCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.VARCHAR, DataType.VARSTRING, DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType()) && type.isAssignableFrom(Clob.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Clob;
  }

  @Override
  public Clob decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Clob> type) {
    String rawValue = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    return Clob.from(Mono.just(rawValue));
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, Clob value) {
    BufferUtils.write(buf, value, context);
  }

  @Override
  public String toString() {
    return "ClobCodec{}";
  }
}

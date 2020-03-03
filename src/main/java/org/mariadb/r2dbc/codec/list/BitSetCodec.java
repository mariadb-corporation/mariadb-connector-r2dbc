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

import java.util.BitSet;

public class BitSetCodec implements Codec<BitSet> {

  public static final BitSetCodec INSTANCE = new BitSetCodec();

  public static BitSet parseBit(ByteBuf buf, int length) {
    byte[] arr = new byte[length];
    buf.readBytes(arr);
    return BitSet.valueOf(arr);
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getDataType() == DataType.BIT && type.isAssignableFrom(BitSet.class);
  }

  @Override
  public BitSet decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BitSet> type) {
    return parseBit(buf, length);
  }

  public boolean canEncode(Object value) {
    return value instanceof BitSet;
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, BitSet value) {
    BufferUtils.write(buf, value);
  }

  @Override
  public String toString() {
    return "BitSetCodec{}";
  }
}

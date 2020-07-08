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
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class BitSetCodec implements Codec<BitSet> {

  public static final BitSetCodec INSTANCE = new BitSetCodec();

  public static BitSet parseBit(ByteBuf buf, int length) {
    byte[] arr = new byte[length];
    buf.readBytes(arr);
    revertOrder(arr);
    return BitSet.valueOf(arr);
  }

  public static void revertOrder(byte[] array) {
    int i = 0;
    int j = array.length - 1;
    byte tmp;
    while (j > i) {
      tmp = array[j];
      array[j] = array[i];
      array[i] = tmp;
      j--;
      i++;
    }
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getType() == DataType.BIT && type.isAssignableFrom(BitSet.class);
  }

  @Override
  public BitSet decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BitSet> type) {
    return parseBit(buf, length);
  }

  @Override
  public BitSet decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BitSet> type) {
    return parseBit(buf, length);
  }

  public boolean canEncode(Object value) {
    return value instanceof BitSet;
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, BitSet value) {
    byte[] bytes = value.toByteArray();
    revertOrder(bytes);

    StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
    sb.append("b'");
    for (int i = 0; i < Byte.SIZE * bytes.length; i++)
      sb.append((bytes[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
    sb.append("'");
    buf.writeCharSequence(sb.toString(), StandardCharsets.US_ASCII);
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, BitSet value) {
    byte[] bytes = value.toByteArray();
    revertOrder(bytes);
    BufferUtils.writeLengthEncode(bytes.length, buf);
    buf.writeBytes(bytes);
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}

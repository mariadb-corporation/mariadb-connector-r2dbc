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
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ClobCodec implements Codec<Clob> {

  public static final ClobCodec INSTANCE = new ClobCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.VARCHAR, DataType.VARSTRING, DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && (type.isAssignableFrom(Clob.class));
  }

  public boolean canEncode(Class<?> value) {
    return Clob.class.isAssignableFrom(value);
  }

  @Override
  public Clob decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Clob> type) {
    String rawValue = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    return Clob.from(Mono.just(rawValue));
  }

  @Override
  public Clob decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Clob> type) {
    String rawValue = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    return Clob.from(Mono.just(rawValue));
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Clob value) {
    buf.writeByte('\'');
    Flux.from(value.stream())
        .handle(
            (tempVal, sync) -> {
              BufferUtils.write(buf, tempVal.toString(), false, context);
              sync.next(buf);
            })
        .subscribe();
    buf.writeByte('\'');
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Clob value) {
    buf.writeByte(0xfe);
    int initialPos = buf.writerIndex();
    buf.writerIndex(buf.writerIndex() + 8); // reserve length encoded length bytes
    Flux.from(value.stream())
        .handle(
            (tempVal, sync) -> {
              buf.writeCharSequence(tempVal, StandardCharsets.UTF_8);
              sync.next(buf);
            })
        .doOnComplete(
            () -> {
              // Write length
              int endPos = buf.writerIndex();
              buf.writerIndex(initialPos);
              buf.writeLongLE(endPos - (initialPos + 8));
              buf.writerIndex(endPos);
            })
        .subscribe();
  }

  public DataType getBinaryEncodeType() {
    return DataType.VARSTRING;
  }
}

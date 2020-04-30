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
import io.r2dbc.spi.Blob;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class BlobCodec implements Codec<Blob> {

  public static final BlobCodec INSTANCE = new BlobCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.BLOB, DataType.TINYBLOB, DataType.MEDIUMBLOB, DataType.LONGBLOB);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType()) && type.isAssignableFrom(Blob.class);
  }

  @Override
  public Blob decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Blob> type) {
    return Blob.from(Mono.just(buf.readSlice(length).nioBuffer()));
  }

  @Override
  public Blob decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Blob> type) {
    return Blob.from(Mono.just(buf.readSlice(length).nioBuffer()));
  }

  public boolean canEncode(Object value) {
    return value instanceof Blob;
  }

  @Override
  public void encodeText(ByteBuf buf, ConnectionContext context, Blob value) {
    BufferUtils.write(buf, value, context);
  }

  @Override
  public void encodeBinary(ByteBuf buf, ConnectionContext context, Blob value) {
    buf.writeByte(0xfe);
    int initialPos = buf.writerIndex();
    buf.writerIndex(buf.writerIndex() + 8); // reserve length encoded length bytes

    Flux.from(value.stream())
        .handle(
            (tempVal, sync) -> {
              buf.writeBytes(tempVal);
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
    return DataType.BLOB;
  }

  @Override
  public String toString() {
    return "BlobCodec{}";
  }
}

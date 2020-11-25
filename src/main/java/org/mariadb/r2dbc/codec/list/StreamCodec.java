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
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class StreamCodec implements Codec<InputStream> {

  public static final StreamCodec INSTANCE = new StreamCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(InputStream.class);
  }

  @Override
  public InputStream decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends InputStream> type) {
    // STRING, VARCHAR, VARSTRING, BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB:
    return new ByteBufInputStream(buf.readRetainedSlice(length), true);
  }

  @Override
  public InputStream decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends InputStream> type) {
    return new ByteBufInputStream(buf.readRetainedSlice(length), true);
  }

  public boolean canEncode(Class<?> value) {
    return InputStream.class.isAssignableFrom(value);
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, InputStream is) {
    try {
      buf.writeBytes("_binary '".getBytes(StandardCharsets.US_ASCII));
      byte[] array = new byte[4096];
      int len;
      while ((len = is.read(array)) > 0) {
        BufferUtils.writeEscaped(buf, array, 0, len, context);
      }
      buf.writeByte('\'');
    } catch (IOException ioe) {
      throw new R2dbcNonTransientResourceException("Failed to read InputStream", ioe);
    }
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, InputStream value) {

    // reserve place for length
    buf.writeByte(0xfe);
    int initialPos = buf.writerIndex();
    buf.writerIndex(buf.writerIndex() + 8);

    byte[] array = new byte[4096];
    int len;
    try {
      while ((len = value.read(array)) > 0) {
        buf.writeBytes(array, 0, len);
      }
    } catch (IOException ioe) {
      throw new R2dbcNonTransientResourceException("Failed to read InputStream", ioe);
    }

    // Write length
    int endPos = buf.writerIndex();
    buf.writerIndex(initialPos);
    buf.writeLongLE(endPos - (initialPos + 8));
    buf.writerIndex(endPos);
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}

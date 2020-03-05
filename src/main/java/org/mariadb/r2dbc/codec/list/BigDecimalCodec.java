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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class BigDecimalCodec implements Codec<BigDecimal> {

  public static final BigDecimalCodec INSTANCE = new BigDecimalCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.BIT,
          DataType.DECIMAL);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(BigDecimal.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof BigDecimal;
  }

  @Override
  public BigDecimal decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends BigDecimal> type) {
    if (column.getDataType() == DataType.BIT) {
      return BigDecimal.valueOf(ByteCodec.parseBit(buf, length));
    }
    String value = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    return new BigDecimal(value);
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, BigDecimal value) {
    BufferUtils.writeAscii(buf, value.toPlainString());
  }

  @Override
  public String toString() {
    return "BigDecimalCodec{}";
  }
}

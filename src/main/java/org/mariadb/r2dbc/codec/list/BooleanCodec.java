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
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;

public class BooleanCodec implements Codec<Boolean> {

  public static final BooleanCodec INSTANCE = new BooleanCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.BIGINT,
          DataType.INTEGER,
          DataType.MEDIUMINT,
          DataType.SMALLINT,
          DataType.TINYINT,
          DataType.DECIMAL,
          DataType.OLDDECIMAL,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIT);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Boolean.TYPE) || type.isAssignableFrom(Boolean.class));
  }

  public boolean canEncode(Class<?> value) {
    return Boolean.class.isAssignableFrom(value);
  }

  @Override
  public Boolean decodeText(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Boolean> type) {
    switch (column.getType()) {
      case BIT:
        return ByteCodec.parseBit(buf, length) != 0;

      case DECIMAL:
      case OLDDECIMAL:
      case FLOAT:
      case DOUBLE:
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString())
                .intValue()
            != 0;

      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        String val = buf.readCharSequence(length, StandardCharsets.US_ASCII).toString();
        return !"0".equals(val);

      default:
        // VARCHAR, VARSTRING, STRING:
        String s = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        return !"0".equals(s);
    }
  }

  @Override
  public Boolean decodeBinary(
      ByteBuf buf, int length, ColumnDefinitionPacket column, Class<? extends Boolean> type) {

    switch (column.getType()) {
      case BIT:
        return ByteCodec.parseBit(buf, length) != 0;

      case DECIMAL:
      case OLDDECIMAL:
        return new BigDecimal(buf.readCharSequence(length, StandardCharsets.US_ASCII).toString())
                .intValue()
            != 0;
      case FLOAT:
        return ((int) buf.readFloatLE()) != 0;

      case DOUBLE:
        return ((int) buf.readDoubleLE()) != 0;

      case TINYINT:
        return buf.readByte() != 0;

      case YEAR:
      case SMALLINT:
        return buf.readShortLE() != 0;

      case MEDIUMINT:
        return buf.readMediumLE() != 0;
      case INTEGER:
        return buf.readIntLE() != 0;
      case BIGINT:
        return buf.readLongLE() != 0;

      default:
        // VARCHAR, VARSTRING, STRING:
        return !"0".equals(buf.readCharSequence(length, StandardCharsets.UTF_8).toString());
    }
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Boolean value) {
    BufferUtils.writeAscii(buf, value ? "1" : "0");
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Boolean value) {
    buf.writeByte(value ? 1 : 0);
  }

  public DataType getBinaryEncodeType() {
    return DataType.TINYINT;
  }
}

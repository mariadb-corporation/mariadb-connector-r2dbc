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
import java.util.EnumSet;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class BinaryRowDecoder extends RowDecoder {

  private int columnNumber;
  private ColumnDefinitionPacket[] columns;
  private byte[] nullBitmap;

  public BinaryRowDecoder(int columnNumber, ColumnDefinitionPacket[] columns) {
    super();
    this.columns = columns;
    this.columnNumber = columnNumber;
  }

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
      return defaultCodec.decodeBinary(buf, length, column, type);
    }

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canDecode(column, type)) {
        return ((Codec<T>) codec).decodeBinary(buf, length, column, type);
      }
    }

    if (type.isArray()) {
      if (EnumSet.of(
              DataType.TINYINT,
              DataType.SMALLINT,
              DataType.MEDIUMINT,
              DataType.INTEGER,
              DataType.BIGINT)
          .contains(column.getDataType())) {
        throw new IllegalArgumentException(
            String.format(
                "No decoder for type %s[] and column type %s(%s)",
                type.getComponentType().getName(),
                column.getDataType().toString(),
                column.isSigned() ? "signed" : "unsigned"));
      }
      throw new IllegalArgumentException(
          String.format(
              "No decoder for type %s[] and column type %s",
              type.getComponentType().getName(), column.getDataType().toString()));
    }
    if (EnumSet.of(
            DataType.TINYINT,
            DataType.SMALLINT,
            DataType.MEDIUMINT,
            DataType.INTEGER,
            DataType.BIGINT)
        .contains(column.getDataType())) {
      throw new IllegalArgumentException(
          String.format(
              "No decoder for type %s and column type %s(%s)",
              type.getName(),
              column.getDataType().toString(),
              column.isSigned() ? "signed" : "unsigned"));
    }
    throw new IllegalArgumentException(
        String.format(
            "No decoder for type %s and column type %s",
            type.getName(), column.getDataType().toString()));
  }

  @Override
  public void resetRow(ByteBuf buf) {
    super.resetRow(buf);
    nullBitmap = new byte[(columnNumber + 9) / 8];
    this.buf.skipBytes(1); // skip 0x00 header
    this.buf.readBytes(nullBitmap);
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (0 is first).
   */
  public void setPosition(int newIndex) {

    // check NULL-Bitmap that indicate if field is null
    if ((nullBitmap[(newIndex + 2) / 8] & (1 << ((newIndex + 2) % 8))) != 0) {
      length = NULL_LENGTH;
      return;
    }

    if (index >= newIndex) {
      index = 0;
      buf.resetReaderIndex();
      // check NULL-Bitmap that indicate if field is null
      buf.skipBytes(1 + ((columnNumber + 9) / 8)); // skip header + null bitmap
    } else {
      index++;
    }

    for (; index <= newIndex; index++) {
      if ((nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) == 0) {
        if (index != newIndex) {
          // skip bytes
          switch (columns[index].getDataType()) {
            case BIGINT:
            case DOUBLE:
              buf.skipBytes(8);
              break;

            case INTEGER:
            case MEDIUMINT:
            case FLOAT:
              buf.skipBytes(4);
              break;

            case SMALLINT:
            case YEAR:
              buf.skipBytes(2);
              break;

            case TINYINT:
              buf.skipBytes(1);
              break;

            default:
              int type = this.buf.readUnsignedByte();
              switch (type) {
                case 251:
                  break;

                case 252:
                  this.buf.skipBytes(this.buf.readUnsignedShortLE());
                  break;

                case 253:
                  this.buf.skipBytes(this.buf.readUnsignedMediumLE());
                  break;

                case 254:
                  this.buf.skipBytes((int) this.buf.readLongLE());
                  break;

                default:
                  this.buf.skipBytes(type);
                  break;
              }
              break;
          }
        } else {
          // read asked field position and length
          switch (columns[index].getDataType()) {
            case BIGINT:
            case DOUBLE:
              length = 8;
              return;

            case INTEGER:
            case MEDIUMINT:
            case FLOAT:
              length = 4;
              return;

            case SMALLINT:
            case YEAR:
              length = 2;
              return;

            case TINYINT:
              length = 1;
              return;

            default:
              // field with variable length
              int type = this.buf.readUnsignedByte();
              switch (type) {
                case 251:
                  // null length field
                  // must never occur
                  // null value are set in NULL-Bitmap, not send with a null length indicator.
                  throw new IllegalStateException(
                      "null data is encoded in binary protocol but NULL-Bitmap is not set");

                case 252:
                  // length is encoded on 3 bytes (0xfc header + 2 bytes indicating length)
                  length = this.buf.readUnsignedShortLE();
                  return;

                case 253:
                  // length is encoded on 4 bytes (0xfd header + 3 bytes indicating length)
                  length = this.buf.readUnsignedMediumLE();
                  return;

                case 254:
                  // length is encoded on 9 bytes (0xfe header + 8 bytes indicating length)
                  length = (int) this.buf.readLongLE();
                  return;

                default:
                  // length is encoded on 1 bytes (is then less than 251)
                  length = type;
                  return;
              }
          }
        }
      }
    }
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec;

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class TextRowDecoder extends RowDecoder {

  public TextRowDecoder(
      int columnNumber, ColumnDefinitionPacket[] columns, MariadbConnectionConfiguration conf) {
    super(conf);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(int index, ColumnDefinitionPacket column, Class<T> type)
      throws IllegalArgumentException {
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
      Codec<T> defaultCodec = ((Codec<T>) column.getDefaultCodec(conf));
      return defaultCodec.decodeText(buf, length, column, type);
    }

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canDecode(column, type)) {
        return ((Codec<T>) codec).decodeText(buf, length, column, type);
      }
    }

    buf.skipBytes(length);

    throw noDecoderException(column, type);
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (0 is first).
   */
  public void setPosition(int newIndex) {
    if (index >= newIndex) {
      index = -1;
      buf.resetReaderIndex();
    }

    index++;

    for (; index < newIndex; index++) {
      int type = this.buf.readUnsignedByte();
      switch (type) {
        case 252:
          buf.skipBytes(buf.readUnsignedShortLE());
          break;
        case 253:
          buf.skipBytes(buf.readUnsignedMediumLE());
          break;
        case 254:
          buf.skipBytes((int) (buf.readLongLE()));
          break;
        case 251:
          break;
        default:
          buf.skipBytes(type);
          break;
      }
    }
    short type = this.buf.readUnsignedByte();
    switch (type) {
      case 251:
        length = NULL_LENGTH;
        break;
      case 252:
        length = buf.readUnsignedShortLE();
        break;
      case 253:
        length = buf.readUnsignedMediumLE();
        break;
      case 254:
        length = (int) buf.readLongLE();
        break;
      default:
        length = type;
        break;
    }
  }
}

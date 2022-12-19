// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;
import reactor.util.annotation.Nullable;

public class MariadbRowBinary extends MariadbRow implements org.mariadb.r2dbc.api.MariadbRow {
  private final int columnNumber;
  private final byte[] nullBitmap;

  public MariadbRowBinary(ByteBuf buf, MariadbRowMetadata meta, ExceptionFactory factory) {
    super(buf, meta, factory);
    columnNumber = meta.size();
    nullBitmap = new byte[(columnNumber + 9) / 8];
    buf.skipBytes(1); // skip 0x00 header
    buf.readBytes(nullBitmap);
    buf.markReaderIndex();
  }

  public MariadbRowMetadata getMetadata() {
    return meta;
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int index, Class<T> type) {

    // check NULL-Bitmap that indicate if field is null
    if ((nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) != 0) {
      if (type != null && type.isPrimitive()) {
        throw new R2dbcTransientResourceException(
            String.format("Cannot return null for primitive %s", type.getName()));
      }
      return null;
    }

    ColumnDefinitionPacket column = meta.getColumnMetadata(index);
    this.setPosition(index);
    if (length == NULL_LENGTH) {
      if (type.isPrimitive()) {
        throw new R2dbcTransientResourceException(
            String.format("Cannot return null for primitive %s", type.getName()));
      }
      return null;
    }

    Codec<T> defaultCodec;
    // type generic, return "natural" java type
    if (Object.class == type || type == null) {
      defaultCodec = ((Codec<T>) column.getType().getDefaultCodec());
      return defaultCodec.decodeBinary(buf, length, column, type, factory);
    }

    // fast path checking default codec
    if ((defaultCodec = (Codec<T>) Codecs.typeMapper.get(type)) != null && defaultCodec.canDecode(column, type)) {
      return defaultCodec.decodeBinary(buf, length, column, type, factory);
    }

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canDecode(column, type)) {
        return ((Codec<T>) codec).decodeBinary(buf, length, column, type, factory);
      }
    }

    buf.skipBytes(length);

    throw MariadbRow.noDecoderException(column, type);
  }

  @Nullable
  @Override
  public <T> T get(String name, Class<T> type) {
    Assert.requireNonNull(name, "name must not be null");
    return get(this.meta.getIndex(name), type);
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (0 is first).
   */
  public void setPosition(int newIndex) {

    if (index >= newIndex) {
      index = 0;
      buf.resetReaderIndex();
    } else {
      index++;
    }

    for (; index < newIndex; index++) {
      if ((nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) == 0) {
        // skip bytes
        switch (meta.get(index).getDataType()) {
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
      }
    }

    // read asked field position and length
    switch (meta.get(index).getDataType()) {
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
        int len = this.buf.readUnsignedByte();
        switch (len) {
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
            // length is encoded on 1 byte (is then less than 251)
            length = len;
        }
    }
  }
}

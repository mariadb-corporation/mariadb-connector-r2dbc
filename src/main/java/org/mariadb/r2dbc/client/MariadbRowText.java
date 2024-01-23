// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;
import reactor.util.annotation.Nullable;

public class MariadbRowText extends MariadbRow implements org.mariadb.r2dbc.api.MariadbRow {

  public MariadbRowText(ByteBuf buf, MariadbRowMetadata meta, ExceptionFactory factory) {
    super(buf, meta, factory);
    this.buf.markReaderIndex();
  }

  public MariadbRowMetadata getMetadata() {
    return meta;
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int index, Class<T> type) {
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
      return defaultCodec.decodeText(buf, length, column, type, factory);
    }

    // fast path checking default codec
    if ((defaultCodec = (Codec<T>) Codecs.typeMapper.get(type)) != null) {
      if (!defaultCodec.canDecode(column, type)) {
        buf.skipBytes(length);
        throw MariadbRow.noDecoderException(column, type);
      }
      return defaultCodec.decodeText(buf, length, column, type, factory);
    }

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canDecode(column, type)) {
        return ((Codec<T>) codec).decodeText(buf, length, column, type, factory);
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

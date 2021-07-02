// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec;

import io.netty.buffer.ByteBuf;
import java.util.EnumSet;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public abstract class RowDecoder {
  protected static final int NULL_LENGTH = -1;

  public ByteBuf buf;
  protected int length;
  protected int index;
  protected MariadbConnectionConfiguration conf;

  public RowDecoder(MariadbConnectionConfiguration conf) {
    this.conf = conf;
  }

  public void resetRow(ByteBuf buf) {
    this.buf = buf;
    this.buf.markReaderIndex();
    index = -1;
  }

  protected IllegalArgumentException noDecoderException(
      ColumnDefinitionPacket column, Class<?> type) {

    if (type.isArray()) {
      if (EnumSet.of(
              DataType.TINYINT,
              DataType.SMALLINT,
              DataType.MEDIUMINT,
              DataType.INTEGER,
              DataType.BIGINT)
          .contains(column.getType())) {
        throw new IllegalArgumentException(
            String.format(
                "No decoder for type %s[] and column type %s(%s)",
                type.getComponentType().getName(),
                column.getType().toString(),
                column.isSigned() ? "signed" : "unsigned"));
      }
      throw new IllegalArgumentException(
          String.format(
              "No decoder for type %s[] and column type %s",
              type.getComponentType().getName(), column.getType().toString()));
    }
    if (EnumSet.of(
            DataType.TINYINT,
            DataType.SMALLINT,
            DataType.MEDIUMINT,
            DataType.INTEGER,
            DataType.BIGINT)
        .contains(column.getType())) {
      throw new IllegalArgumentException(
          String.format(
              "No decoder for type %s and column type %s(%s)",
              type.getName(),
              column.getType().toString(),
              column.isSigned() ? "signed" : "unsigned"));
    }
    throw new IllegalArgumentException(
        String.format(
            "No decoder for type %s and column type %s",
            type.getName(), column.getType().toString()));
  }

  public abstract void setPosition(int position);

  @SuppressWarnings("unchecked")
  public abstract <T> T get(int index, ColumnDefinitionPacket column, Class<T> type)
      throws IllegalArgumentException;
}

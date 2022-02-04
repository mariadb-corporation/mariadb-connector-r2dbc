// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec;

import io.netty.buffer.ByteBuf;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;

public abstract class RowDecoder {
  protected static final int NULL_LENGTH = -1;

  public ByteBuf buf;
  protected int length;
  protected int index;
  protected MariadbConnectionConfiguration conf;
  protected ExceptionFactory factory;

  public RowDecoder(MariadbConnectionConfiguration conf, ExceptionFactory factory) {
    Assert.requireNonNull(factory, "missing factory parameter");
    this.conf = conf;
    this.factory = factory;
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

  public abstract void setPosition(int position);

  @SuppressWarnings("unchecked")
  public abstract <T> T get(int index, ColumnDefinitionPacket column, Class<T> type)
      throws IllegalArgumentException;
}

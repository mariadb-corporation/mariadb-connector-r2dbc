// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.util.*;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;
import reactor.util.annotation.Nullable;

public abstract class MariadbRow {
  protected static final int NULL_LENGTH = -1;
  protected int length;
  protected int index = -1;

  protected final MariadbRowMetadata meta;
  protected final ByteBuf buf;
  protected final ExceptionFactory factory;

  MariadbRow(ByteBuf buf, MariadbRowMetadata meta, ExceptionFactory factory) {
    this.buf = buf;
    this.meta = meta;
    this.factory = factory;
  }

  @FunctionalInterface
  public interface MariadbRowConstructor {

    org.mariadb.r2dbc.api.MariadbRow create(
        ByteBuf buf, MariadbRowMetadata meta, ExceptionFactory factory);
  }

  public abstract <T> T get(int index, Class<T> type);

  @Nullable
  public <T> T get(String name, Class<T> type) {
    Assert.requireNonNull(name, "name must not be null");
    return get(this.meta.getIndex(name), type);
  }

  protected static R2dbcTransientResourceException noDecoderException(
      ColumnDefinitionPacket column, Class<?> type) {

    if (type.isArray()) {
      if (EnumSet.of(
              DataType.TINYINT,
              DataType.SMALLINT,
              DataType.MEDIUMINT,
              DataType.INTEGER,
              DataType.BIGINT)
          .contains(column.getDataType())) {
        throw new R2dbcTransientResourceException(
            String.format(
                "No decoder for type %s[] and column type %s(%s)",
                type.getComponentType().getName(),
                column.getDataType().toString(),
                column.isSigned() ? "signed" : "unsigned"));
      }
      throw new R2dbcTransientResourceException(
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
      throw new R2dbcTransientResourceException(
          String.format(
              "No decoder for type %s and column type %s(%s)",
              type.getName(),
              column.getDataType().toString(),
              column.isSigned() ? "signed" : "unsigned"));
    }
    throw new R2dbcTransientResourceException(
        String.format(
            "No decoder for type %s and column type %s",
            type.getName(), column.getDataType().toString()));
  }
}

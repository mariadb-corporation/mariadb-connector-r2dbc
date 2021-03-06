// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Row;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import org.mariadb.r2dbc.codec.RowDecoder;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;
import reactor.util.annotation.Nullable;

public class MariadbRow implements Row {

  private final ColumnDefinitionPacket[] columnDefinitionPackets;
  private final RowDecoder decoder;
  private final ByteBuf raw;

  MariadbRow(ColumnDefinitionPacket[] columnDefinitionPackets, RowDecoder decoder, ByteBuf data) {
    this.columnDefinitionPackets = columnDefinitionPackets;
    this.decoder = decoder;
    this.raw = data;

    decoder.resetRow(raw);
  }

  @Nullable
  @Override
  public <T> T get(int index, Class<T> type) {
    Assert.requireNonNull(type, "type must not be null");
    return decoder.get(index, getMeta(index), type);
  }

  @Nullable
  @Override
  public <T> T get(String name, Class<T> type) {
    Assert.requireNonNull(name, "name must not be null");
    Assert.requireNonNull(type, "type must not be null");
    return get(getColumn(name), type);
  }

  private int getColumn(String name) {
    Assert.requireNonNull(name, "name must not be null");
    for (int i = 0; i < this.columnDefinitionPackets.length; i++) {
      if (this.columnDefinitionPackets[i].getColumnAlias().equalsIgnoreCase(name)) {
        return i;
      }
    }

    Set<String> columnNames = new TreeSet<>();
    for (ColumnDefinitionPacket columnDef : columnDefinitionPackets) {
      columnNames.add(columnDef.getColumnAlias());
    }
    throw new IllegalArgumentException(
        String.format(
            "Column name '%s' does not exist in column names %s",
            name, Collections.unmodifiableCollection(columnNames)));
  }

  private ColumnDefinitionPacket getMeta(int index) {
    if (index < 0) {
      throw new IllegalArgumentException(String.format("Column index %d must be positive", index));
    }
    if (index >= this.columnDefinitionPackets.length) {
      throw new IllegalArgumentException(
          String.format(
              "Column index %d not in range [0-%s]",
              index, this.columnDefinitionPackets.length - 1));
    }
    return this.columnDefinitionPackets[index];
  }
}

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

package org.mariadb.r2dbc;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Row;
import org.mariadb.r2dbc.codec.RowDecoder;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

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
    ColumnDefinitionPacket columnMeta = getMeta(index);
    return decode(index, columnMeta, type);
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
              "Column index %d is larger than the number of columns %d",
              index, this.columnDefinitionPackets.length));
    }
    return this.columnDefinitionPackets[index];
  }

  @Nullable
  private <T> T decode(int index, ColumnDefinitionPacket column, Class<T> type) {
    return decoder.get(index, column, type);
  }
}

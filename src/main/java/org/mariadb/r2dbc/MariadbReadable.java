// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Readable;
import java.util.*;
import org.mariadb.r2dbc.codec.RowDecoder;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;
import reactor.util.annotation.Nullable;

public class MariadbReadable implements Readable {

  private final RowDecoder decoder;
  protected final List<ColumnDefinitionPacket> metadataList;
  private ByteBuf raw;

  MariadbReadable(RowDecoder decoder, List<ColumnDefinitionPacket> metadataList) {
    this.decoder = decoder;
    this.metadataList = metadataList;
  }

  protected void updateRaw(ByteBuf data) {
    this.raw = data;
    decoder.resetRow(raw);
  }

  @Nullable
  @Override
  public <T> T get(int index, Class<T> type) {
    return decoder.get(index, getColumnsDef(index), type);
  }

  private ColumnDefinitionPacket getColumnsDef(int index) {
    if (index < 0) {
      throw new IndexOutOfBoundsException(String.format("Column index %d must be positive", index));
    }
    if (index >= this.metadataList.size()) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Column index %d not in range [0-%s]", index, this.metadataList.size() - 1));
    }
    return this.metadataList.get(index);
  }

  private int getIndex(String name) {
    Assert.requireNonNull(name, "name must not be null");
    for (int i = 0; i < this.metadataList.size(); i++) {
      if (this.metadataList.get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }

    Set<String> columnNames = new TreeSet<>();
    for (ColumnDefinitionPacket columnDef : this.metadataList) {
      columnNames.add(columnDef.getName());
    }
    throw new NoSuchElementException(
        String.format(
            "Column name '%s' does not exist in column names %s",
            name, Collections.unmodifiableCollection(columnNames)));
  }

  @Nullable
  @Override
  public <T> T get(String name, Class<T> type) {
    Assert.requireNonNull(name, "name must not be null");
    return get(getIndex(name), type);
  }
}

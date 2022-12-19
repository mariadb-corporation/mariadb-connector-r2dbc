// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.r2dbc.spi.RowMetadata;
import java.util.*;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;

public final class MariadbRowMetadata implements RowMetadata {

  private final ColumnDefinitionPacket[] metadataList;
  private volatile Collection<String> columnNames;
  private Map<String, Integer> mapper = null;

  public MariadbRowMetadata(ColumnDefinitionPacket[] metadataList) {
    this.metadataList = metadataList;
  }

  @Override
  public ColumnDefinitionPacket getColumnMetadata(int index) {
    if (index < 0 || index >= this.metadataList.length) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Column index %d is not in permit range[0,%s]", index, this.metadataList.length - 1));
    }
    return this.metadataList[index];
  }

  @Override
  public ColumnDefinitionPacket getColumnMetadata(String name) {
    return metadataList[getIndex(name)];
  }

  @Override
  public List<ColumnDefinitionPacket> getColumnMetadatas() {
    return Arrays.asList(this.metadataList);
  }

  private Collection<String> getColumnNames(ColumnDefinitionPacket[] columnMetadatas) {
    List<String> columnNames = new ArrayList<>();
    for (ColumnDefinitionPacket columnMetadata : columnMetadatas) {
      columnNames.add(columnMetadata.getName());
    }
    return Collections.unmodifiableCollection(columnNames);
  }

  @Override
  public String toString() {
    if (this.columnNames == null) {
      this.columnNames = getColumnNames(this.metadataList);
    }
    return "MariadbRowMetadata{columnNames=" + columnNames + "}";
  }

  @Override
  public boolean contains(String columnName) {
    if (this.columnNames == null) {
      this.columnNames = getColumnNames(this.metadataList);
    }
    return this.columnNames.stream().anyMatch(columnName::equalsIgnoreCase);
  }

  public int size() {
    return this.metadataList.length;
  }

  ColumnDefinitionPacket get(int index) {
    return this.metadataList[index];
  }

  int getIndex(String name) throws NoSuchElementException {
    Assert.requireNonNull(name, "name must not be null");

    if (mapper == null) {
      Map<String, Integer> tmpmapper = new HashMap<>();
      for (int i = 0; i < metadataList.length; i++) {
        ColumnDefinitionPacket ci = metadataList[i];
        String columnAlias = ci.getName();
        if (columnAlias == null || columnAlias.isEmpty()) {
          String columnName = ci.getColumn();
          if (columnName != null && !columnName.isEmpty()) {
            columnName = columnName.toLowerCase(Locale.ROOT);
            tmpmapper.putIfAbsent(columnName, i);
          }
        } else {
          tmpmapper.putIfAbsent(columnAlias.toLowerCase(Locale.ROOT), i);
        }
      }
      mapper = tmpmapper;
    }

    Integer ind = mapper.get(name.toLowerCase(Locale.ROOT));
    if (ind == null) {
      throw new NoSuchElementException(
          String.format(
              "Column name '%s' does not exist in column names %s",
              name, Collections.unmodifiableCollection(mapper.keySet())));
    }
    return ind;
  }
}

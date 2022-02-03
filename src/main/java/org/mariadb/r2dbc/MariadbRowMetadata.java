// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.RowMetadata;
import java.util.*;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;

final class MariadbRowMetadata implements RowMetadata {

  private final List<ColumnDefinitionPacket> metadataList;
  private volatile Collection<String> columnNames;

  MariadbRowMetadata(List<ColumnDefinitionPacket> metadataList) {
    this.metadataList = metadataList;
  }

  @Override
  public ColumnDefinitionPacket getColumnMetadata(int index) {
    if (index < 0 || index >= this.metadataList.size()) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Column index %d is not in permit range[0,%s]", index, this.metadataList.size() - 1));
    }
    return this.metadataList.get(index);
  }

  @Override
  public ColumnDefinitionPacket getColumnMetadata(String name) {
    return metadataList.get(getColumn(name));
  }

  private int getColumn(String name) {
    Assert.requireNonNull(name, "name must not be null");
    for (int i = 0; i < this.metadataList.size(); i++) {
      if (this.metadataList.get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }
    throw new NoSuchElementException(
        String.format(
            "Column name '%s' does not exist in column names %s", name, getColumnNames()));
  }

  @Override
  public List<ColumnDefinitionPacket> getColumnMetadatas() {
    return Collections.unmodifiableList(this.metadataList);
  }

  @Override
  @Deprecated
  public Collection<String> getColumnNames() {
    if (this.columnNames == null) {
      this.columnNames = getColumnNames(this.metadataList);
    }
    return Collections.unmodifiableCollection(this.columnNames);
  }

  private Collection<String> getColumnNames(List<ColumnDefinitionPacket> columnMetadatas) {
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
    StringBuilder sb = new StringBuilder("MariadbRowMetadata{");
    sb.append("columnNames=").append(columnNames).append("}");
    return sb.toString();
  }

  @Override
  public boolean contains(String columnName) {
    if (this.columnNames == null) {
      this.columnNames = getColumnNames(this.metadataList);
    }
    return this.columnNames.stream().anyMatch(columnName::equalsIgnoreCase);
  }
}

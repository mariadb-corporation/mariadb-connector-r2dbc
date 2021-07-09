// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;

final class MariadbOutParametersMetadata implements OutParametersMetadata {

  private final List<ColumnDefinitionPacket> metadataList;
  private volatile Collection<String> columnNames;

  MariadbOutParametersMetadata(List<ColumnDefinitionPacket> metadataList) {
    this.metadataList = metadataList;
  }

  @Override
  public ColumnDefinitionPacket getParameterMetadata(int index) {
    if (index < 0 || index >= this.metadataList.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Column index %d is not in permit range[0,%s]", index, this.metadataList.size() - 1));
    }
    return this.metadataList.get(index);
  }

  @Override
  public ColumnDefinitionPacket getParameterMetadata(String name) {
    return metadataList.get(getIndex(name));
  }

  @Override
  public List<? extends OutParameterMetadata> getParameterMetadatas() {
    return Collections.unmodifiableList(this.metadataList);
  }

  private int getIndex(String name) {
    Assert.requireNonNull(name, "name must not be null");
    for (int i = 0; i < this.metadataList.size(); i++) {
      if (this.metadataList.get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }

    throw new IllegalArgumentException(
        String.format(
            "Column name '%s' does not exist in column names %s",
            name, getColumnNames(this.metadataList)));
  }

  private Collection<String> getColumnNames(List<ColumnDefinitionPacket> columnMetadatas) {
    List<String> columnNames = new ArrayList<>();
    for (ColumnDefinitionPacket columnMetadata : columnMetadatas) {
      columnNames.add(columnMetadata.getName());
    }
    return Collections.unmodifiableCollection(columnNames);
  }
}

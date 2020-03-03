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

import io.r2dbc.spi.RowMetadata;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.Assert;

import java.util.*;

final class MariadbRowMetadata implements RowMetadata {

  private static final Comparator<String> IGNORE_CASE_COMPARATOR =
      (o1, o2) -> o2.compareToIgnoreCase(o1);
  private final List<MariadbColumnMetadata> metadataList;
  private volatile Collection<String> columnNames;

  MariadbRowMetadata(List<MariadbColumnMetadata> metadataList) {
    this.metadataList = metadataList;
  }

  static MariadbRowMetadata toRowMetadata(ColumnDefinitionPacket[] metadataList) {
    List<MariadbColumnMetadata> columnMetadata = new ArrayList<>(metadataList.length);
    for (ColumnDefinitionPacket col : metadataList) {
      columnMetadata.add(new MariadbColumnMetadata(col));
    }
    return new MariadbRowMetadata(columnMetadata);
  }

  @Override
  public MariadbColumnMetadata getColumnMetadata(int index) {
    if (index >= this.metadataList.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Column index %d is larger than the number of columns %d",
              index, this.metadataList.size()));
    }
    return this.metadataList.get(index);
  }

  @Override
  public MariadbColumnMetadata getColumnMetadata(String name) {
    Assert.requireNonNull(name, "name must not be null");
    for (MariadbColumnMetadata metadata : this.metadataList) {
      if (metadata.getName().equalsIgnoreCase(name)) {
        return metadata;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Column name '%s' does not exist in column names %s", name, getColumnNames()));
  }

  public int getColumn(String name) {
    Assert.requireNonNull(name, "name must not be null");
    for (int i = 0; i < this.metadataList.size(); i++) {
      if (this.metadataList.get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Column name '%s' does not exist in column names %s", name, getColumnNames()));
  }

  @Override
  public List<MariadbColumnMetadata> getColumnMetadatas() {
    return Collections.unmodifiableList(this.metadataList);
  }

  @Override
  public Collection<String> getColumnNames() {
    if (this.columnNames == null) {
      this.columnNames = getColumnNames(this.metadataList);
    }
    return Collections.unmodifiableCollection(this.columnNames);
  }

  private Collection<String> getColumnNames(List<MariadbColumnMetadata> columnMetadatas) {
    Set<String> columnNames = new TreeSet<>(IGNORE_CASE_COMPARATOR);
    for (MariadbColumnMetadata columnMetadata : columnMetadatas) {
      columnNames.add(columnMetadata.getName());
    }
    return columnNames;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("MariadbRowMetadata{");
    sb.append("metadataList=[");
    for (MariadbColumnMetadata columnMetadata : metadataList) {
      sb.append(columnMetadata).append(",");
    }
    sb.append("], columnNames=").append(columnNames).append("}");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MariadbRowMetadata that = (MariadbRowMetadata) o;
    return Objects.equals(metadataList, that.metadataList)
        && Objects.equals(columnNames, that.columnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadataList, columnNames);
  }
}

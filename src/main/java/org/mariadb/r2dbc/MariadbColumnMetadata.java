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

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

final class MariadbColumnMetadata implements ColumnMetadata {

  private ColumnDefinitionPacket columnDefinitionPacket;

  MariadbColumnMetadata(ColumnDefinitionPacket columnDefinitionPacket) {
    this.columnDefinitionPacket = columnDefinitionPacket;
  }

  @Override
  public String getName() {
    return columnDefinitionPacket.getColumnAlias();
  }

  @Override
  public Class<?> getJavaType() {
    return columnDefinitionPacket.getDataType().getDeclaringClass();
  }

  @Override
  public ColumnDefinitionPacket getNativeTypeMetadata() {
    return columnDefinitionPacket;
  }

  @Override
  public Nullability getNullability() {
    return this.columnDefinitionPacket.getNullability();
  }

  @Override
  public Integer getPrecision() {
    switch (columnDefinitionPacket.getDataType()) {
      case OLDDECIMAL:
      case DECIMAL:
        // DECIMAL and OLDDECIMAL are  "exact" fixed-point number.
        // so :
        // - if can be signed, 1 byte is saved for sign
        // - if decimal > 0, one byte more for dot
        if (columnDefinitionPacket.isSigned()) {
          return (int)
              (columnDefinitionPacket.getLength()
                  - ((columnDefinitionPacket.getDecimals() > 0) ? 2 : 1));
        } else {
          return (int)
              (columnDefinitionPacket.getLength()
                  - ((columnDefinitionPacket.getDecimals() > 0) ? 1 : 0));
        }
      default:
        return (int) columnDefinitionPacket.getLength();
    }
  }

  @Override
  public Integer getScale() {
    return null;
  }
}

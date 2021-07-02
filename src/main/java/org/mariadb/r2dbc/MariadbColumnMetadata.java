// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

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
    return columnDefinitionPacket.getJavaClass();
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
    switch (columnDefinitionPacket.getType()) {
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
    switch (columnDefinitionPacket.getType()) {
      case OLDDECIMAL:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case FLOAT:
      case DOUBLE:
      case BIGINT:
      case MEDIUMINT:
      case BIT:
      case DECIMAL:
        return (int) columnDefinitionPacket.getDecimals();
      default:
        return 0;
    }
  }
}

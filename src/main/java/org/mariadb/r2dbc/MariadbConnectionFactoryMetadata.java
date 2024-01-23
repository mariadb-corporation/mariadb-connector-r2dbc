// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.ConnectionFactoryMetadata;

final class MariadbConnectionFactoryMetadata implements ConnectionFactoryMetadata {

  public static final String NAME = "MariaDB";

  static final MariadbConnectionFactoryMetadata INSTANCE = new MariadbConnectionFactoryMetadata();

  private MariadbConnectionFactoryMetadata() {}

  @Override
  public String getName() {
    return NAME;
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;

public interface MariadbStatement extends Statement {

  @Override
  MariadbStatement add();

  @Override
  MariadbStatement bind(String identifier, Object value);

  @Override
  MariadbStatement bind(int index, Object value);

  @Override
  MariadbStatement bindNull(String identifier, Class<?> type);

  @Override
  MariadbStatement bindNull(int index, Class<?> type);

  @Override
  Flux<MariadbResult> execute();

  @Override
  default MariadbStatement fetchSize(int rows) {
    return this;
  }

  @Override
  MariadbStatement returnGeneratedValues(String... columns);
}

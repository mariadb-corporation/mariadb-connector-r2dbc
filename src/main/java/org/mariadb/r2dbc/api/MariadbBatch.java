// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

public interface MariadbBatch extends Batch {

  @Override
  MariadbBatch add(String sql);

  @Override
  Flux<MariadbResult> execute();
}

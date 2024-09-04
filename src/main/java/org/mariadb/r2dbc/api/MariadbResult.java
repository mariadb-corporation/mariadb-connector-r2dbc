// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.function.BiFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MariadbResult extends Result {

  @Override
  Mono<Integer> getRowsUpdated();

  @Override
  <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction);
}

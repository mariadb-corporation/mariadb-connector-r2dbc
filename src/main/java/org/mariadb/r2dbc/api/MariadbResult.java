// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.*;
import io.r2dbc.spi.Readable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public interface MariadbResult extends Result {

  @Override
  Flux<Integer> getRowsUpdated();

  @Override
  <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction);

  @Override
  <T> Flux<T> map(Function<? super Readable, ? extends T> mappingFunction);

  @Override
  Result filter(Predicate<Segment> filter);

  @Override
  <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction);
}

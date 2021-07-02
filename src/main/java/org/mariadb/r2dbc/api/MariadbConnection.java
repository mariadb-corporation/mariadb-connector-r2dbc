// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Mono;

public interface MariadbConnection extends Connection {

  @Override
  Mono<Void> beginTransaction();

  @Override
  Mono<Void> close();

  @Override
  Mono<Void> commitTransaction();

  @Override
  MariadbBatch createBatch();

  @Override
  Mono<Void> createSavepoint(String name);

  @Override
  MariadbStatement createStatement(String sql);

  @Override
  MariadbConnectionMetadata getMetadata();

  @Override
  IsolationLevel getTransactionIsolationLevel();

  @Override
  boolean isAutoCommit();

  @Override
  Mono<Void> releaseSavepoint(String name);

  @Override
  Mono<Void> rollbackTransaction();

  @Override
  Mono<Void> rollbackTransactionToSavepoint(String name);

  @Override
  Mono<Void> setAutoCommit(boolean autoCommit);

  @Override
  Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel);

  @Override
  Mono<Boolean> validate(ValidationDepth depth);

  long getThreadId();
}

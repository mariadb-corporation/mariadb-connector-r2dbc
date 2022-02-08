// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.*;
import java.time.Duration;
import reactor.core.publisher.Mono;

public interface MariadbConnection extends Connection {

  @Override
  Mono<Void> beginTransaction();

  @Override
  Mono<Void> beginTransaction(TransactionDefinition definition);

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

  String getDatabase();

  Mono<Void> setDatabase(String database);

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

  @Override
  Mono<Void> setLockWaitTimeout(Duration timeout);

  @Override
  Mono<Void> setStatementTimeout(Duration timeout);

  long getThreadId();

  String getHost();

  int getPort();
}

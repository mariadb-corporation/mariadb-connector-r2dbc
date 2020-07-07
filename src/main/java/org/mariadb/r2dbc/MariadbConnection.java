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

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.ClientBase;
import org.mariadb.r2dbc.message.client.PingPacket;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.PrepareCache;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

final class MariadbConnection implements org.mariadb.r2dbc.api.MariadbConnection {

  private final Logger logger = Loggers.getLogger(this.getClass());
  private final Client client;
  private final MariadbConnectionConfiguration configuration;
  private volatile IsolationLevel isolationLevel;

  MariadbConnection(
      Client client, IsolationLevel isolationLevel, MariadbConnectionConfiguration configuration) {
    this.client = Assert.requireNonNull(client, "client must not be null");
    this.isolationLevel = Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");
    this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");

    // save Global isolation level to avoid asking each new connection with same configuration
    if (configuration.getIsolationLevel() == null) {
      configuration.setIsolationLevel(isolationLevel);
    }
  }

  @Override
  public Mono<Void> beginTransaction() {
    try (ClientBase.LockAction lockAction = this.client.getLockAction()) {
      return lockAction.beginTransaction();
    }
  }

  @Override
  public Mono<Void> close() {
    return this.client.close().then(Mono.empty());
  }

  @Override
  public Mono<Void> commitTransaction() {
    try (ClientBase.LockAction lockAction = this.client.getLockAction()) {
      return lockAction.commitTransaction();
    }
  }

  @Override
  public MariadbBatch createBatch() {
    return new MariadbBatch(this.client, this.configuration);
  }

  @Override
  public Mono<Void> createSavepoint(String name) {
    Assert.requireNonNull(name, "name must not be null");
    try (ClientBase.LockAction lockAction = this.client.getLockAction()) {
      return lockAction.createSavepoint(name);
    }
  }

  @Override
  public MariadbStatement createStatement(String sql) {
    Assert.requireNonNull(sql, "sql must not be null");
    if (sql.trim().isEmpty()) {
      throw new IllegalArgumentException("Statement cannot be empty.");
    }
    if (MariadbSimpleQueryStatement.supports(sql, this.client)) {
      return new MariadbSimpleQueryStatement(this.client, sql);
    } else {
      if (this.configuration.useServerPrepStmts()) {
        return new MariadbServerParameterizedQueryStatement(this.client, sql, this.configuration);
      }
      return new MariadbClientParameterizedQueryStatement(this.client, sql, this.configuration);
    }
  }

  @Override
  public MariadbConnectionMetadata getMetadata() {
    return new MariadbConnectionMetadata(this.client.getVersion());
  }

  @Override
  public IsolationLevel getTransactionIsolationLevel() {
    return this.isolationLevel;
  }

  @Override
  public boolean isAutoCommit() {
    return this.client.isAutoCommit();
  }

  @Override
  public Mono<Void> releaseSavepoint(String name) {
    Assert.requireNonNull(name, "name must not be null");
    try (ClientBase.LockAction lockAction = this.client.getLockAction()) {
      return lockAction.releaseSavepoint(name);
    }
  }

  @Override
  public Mono<Void> rollbackTransaction() {
    try (ClientBase.LockAction lockAction = this.client.getLockAction()) {
      return lockAction.rollbackTransaction();
    }
  }

  @Override
  public Mono<Void> rollbackTransactionToSavepoint(String name) {
    Assert.requireNonNull(name, "name must not be null");
    try (ClientBase.LockAction lockAction = this.client.getLockAction()) {
      return lockAction.rollbackTransactionToSavepoint(name);
    }
  }

  @Override
  public Mono<Void> setAutoCommit(boolean autoCommit) {
    try (ClientBase.LockAction lockAction = this.client.getLockAction()) {
      return lockAction.setAutoCommit(autoCommit);
    }
  }

  @Override
  public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");
    String sql = String.format("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql());
    ExceptionFactory exceptionFactory = ExceptionFactory.withSql(sql);
    return client
        .sendCommand(new QueryPacket(sql))
        .handle(exceptionFactory::handleErrorResponse)
        .then()
        .doOnSuccess(ignore -> this.isolationLevel = isolationLevel);
  }

  @Override
  public String toString() {
    return "MariadbConnection{client=" + client + ", isolationLevel=" + isolationLevel + '}';
  }

  @Override
  public Mono<Boolean> validate(ValidationDepth depth) {
    if (depth == ValidationDepth.LOCAL) {
      return Mono.just(this.client.isConnected());
    }

    return Mono.create(
        sink -> {
          if (!this.client.isConnected()) {
            sink.success(false);
            return;
          }

          this.client
              .sendCommand(new PingPacket())
              .windowUntil(it -> it.ending())
              .subscribe(
                  msg -> sink.success(true),
                  err -> {
                    logger.debug("Ping error", err);
                    sink.success(false);
                  });
        });
  }

  public PrepareCache _test_prepareCache() {
    return client.getPrepareCache();
  }
}

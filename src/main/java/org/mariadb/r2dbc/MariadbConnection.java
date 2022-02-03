// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import java.time.Duration;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.client.Client;
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
    return this.client.beginTransaction();
  }

  @Override
  public Mono<Void> beginTransaction(TransactionDefinition definition) {
    return this.client.beginTransaction(definition);
  }

  @Override
  public Mono<Void> close() {
    return this.client.close().then(Mono.empty());
  }

  @Override
  public Mono<Void> commitTransaction() {
    return this.client.commitTransaction();
  }

  @Override
  public MariadbBatch createBatch() {
    return new MariadbBatch(this.client, this.configuration);
  }

  @Override
  public Mono<Void> createSavepoint(String name) {
    Assert.requireNonNull(name, "name must not be null");
    if (isAutoCommit()) {
      return this.client.beginTransaction().then(this.client.createSavepoint(name));
    }
    return this.client.createSavepoint(name);
  }

  @Override
  public MariadbStatement createStatement(String sql) {
    Assert.requireNonNull(sql, "sql must not be null");
    if (sql.trim().isEmpty()) {
      throw new IllegalArgumentException("Statement cannot be empty.");
    }

    if (this.configuration.useServerPrepStmts() || sql.contains("call")) {
      return new MariadbServerParameterizedQueryStatement(this.client, sql, this.configuration);
    }
    return new MariadbClientParameterizedQueryStatement(this.client, sql, this.configuration);
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
    return this.client.isAutoCommit() && !this.client.isInTransaction();
  }

  @Override
  public Mono<Void> releaseSavepoint(String name) {
    Assert.requireNonNull(name, "name must not be null");
    return this.client.releaseSavepoint(name);
  }

  @Override
  public long getThreadId() {
    return this.client.getThreadId();
  }

  @Override
  public String getHost() {
    return this.client.getHostAddress() != null ? this.client.getHostAddress().getHost() : null;
  }

  @Override
  public int getPort() {
    return this.client.getHostAddress() != null ? this.client.getHostAddress().getPort() : 3306;
  }

  @Override
  public Mono<Void> rollbackTransaction() {
    return this.client.rollbackTransaction();
  }

  @Override
  public Mono<Void> rollbackTransactionToSavepoint(String name) {
    Assert.requireNonNull(name, "name must not be null");
    return this.client.rollbackTransactionToSavepoint(name);
  }

  @Override
  public Mono<Void> setAutoCommit(boolean autoCommit) {
    return client.setAutoCommit(autoCommit);
  }

  @Override
  public Mono<Void> setLockWaitTimeout(Duration timeout) {
    return Mono.empty();
  }

  @Override
  public Mono<Void> setStatementTimeout(Duration timeout) {
    Assert.requireNonNull(timeout, "timeout must not be null");
    boolean serverSupportTimeout =
        (client.getVersion().isMariaDBServer()
                && client.getVersion().versionGreaterOrEqual(10, 1, 1)
            || (!client.getVersion().isMariaDBServer()
                && client.getVersion().versionGreaterOrEqual(5, 7, 4)));
    if (!serverSupportTimeout) {
      return Mono.error(
          ExceptionFactory.createException(
              "query timeout not supported by server. (required MariaDB 10.1.1+ | MySQL 5.7.4+)",
              "HY000",
              -1,
              "SET max_statement_time"));
    }

    long msValue = timeout.toMillis();

    // MariaDB did implement max_statement_time in seconds, MySQL copied feature but in ms ...

    String sql;
    if (client.getVersion().isMariaDBServer()) {
      sql = String.format("SET max_statement_time=%s", (double) msValue / 1000);
    } else {
      sql = String.format("SET SESSION MAX_EXECUTION_TIME=%s", msValue);
    }

    ExceptionFactory exceptionFactory = ExceptionFactory.withSql(sql);
    return client
        .sendCommand(new QueryPacket(sql))
        .handle(exceptionFactory::handleErrorResponse)
        .then();
  }

  @Override
  public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");
    final IsolationLevel newIsolation = isolationLevel;
    String sql = String.format("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql());
    ExceptionFactory exceptionFactory = ExceptionFactory.withSql(sql);
    return client
        .sendCommand(new QueryPacket(sql))
        .handle(exceptionFactory::handleErrorResponse)
        .then()
        .doOnSuccess(ignore -> this.isolationLevel = newIsolation);
  }

  @Override
  public String toString() {
    return "MariadbConnection{client=" + client + ", isolationLevel=" + isolationLevel + '}';
  }

  @Override
  public Mono<Boolean> validate(ValidationDepth depth) {
    if (this.client.isCloseRequested()) {
      return Mono.just(false);
    }
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

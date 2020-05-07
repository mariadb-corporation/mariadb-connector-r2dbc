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

package org.mariadb.r2dbc.client;

import io.netty.channel.Channel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.message.client.QuitPacket;
import org.mariadb.r2dbc.message.client.SslRequestPacket;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;
import org.mariadb.r2dbc.message.server.ServerMessage;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;

public abstract class ClientBase implements Client {

  private static final Logger logger = Loggers.getLogger(ClientBase.class);
  protected final ReentrantLock lock = new ReentrantLock();
  private final MariadbConnectionConfiguration configuration;
  protected final Connection connection;
  protected final Queue<CmdElement> responseReceivers = Queues.<CmdElement>unbounded().get();
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final MariadbPacketDecoder mariadbPacketDecoder;
  private final MariadbPacketEncoder mariadbPacketEncoder = new MariadbPacketEncoder();
  private volatile ConnectionContext context;
  private final PrepareCache prepareCache;

  protected ClientBase(Connection connection, MariadbConnectionConfiguration configuration) {
    this.connection = connection;
    this.configuration = configuration;
    this.prepareCache =
        this.configuration.useServerPrepStmts()
            ? new PrepareCache(this.configuration.getPrepareCacheSize(), this)
            : null;
    this.mariadbPacketDecoder =
        new MariadbPacketDecoder(responseReceivers, this.prepareCache, this);

    connection.addHandler(mariadbPacketDecoder);
    connection.addHandler(mariadbPacketEncoder);

    if (logger.isTraceEnabled()) {
      connection.addHandlerFirst(
          LoggingHandler.class.getSimpleName(),
          new LoggingHandler(ClientBase.class, LogLevel.TRACE));
    }

    connection
        .inbound()
        .receive()
        .doOnError(this::handleConnectionError)
        .doOnComplete(this::closedServlet)
        .then()
        .subscribe();
  }

  private Mono<Void> handleConnectionError(Throwable throwable) {
    clearWaitingListWithError(new MariadbConnectionException(throwable));
    logger.error("Connection Error", throwable);
    return close();
  }

  @Override
  public Mono<Void> close() {
    return Mono.defer(
        () -> {
          clearWaitingListWithError(
              new R2dbcNonTransientResourceException("Connection is closing"));
          if (this.isClosed.compareAndSet(false, true)) {

            Channel channel = this.connection.channel();
            if (!channel.isOpen()) {
              this.connection.dispose();
              return this.connection.onDispose();
            }

            return Flux.just(QuitPacket.INSTANCE)
                .doOnNext(message -> connection.channel().writeAndFlush(message))
                .then()
                .doOnSuccess(v -> this.connection.dispose())
                .then(this.connection.onDispose());
          }

          return Mono.empty();
        });
  }

  public Flux<ServerMessage> sendCommand(ClientMessage message) {
    return sendCommand(message, DecoderState.QUERY_RESPONSE);
  }

  @Override
  public Mono<Void> sendSslRequest(
      SslRequestPacket sslRequest, MariadbConnectionConfiguration configuration) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    try {
      SSLEngine engine =
          configuration.getSslConfig().getSslContext().newEngine(connection.channel().alloc());
      final SslHandler sslHandler = new SslHandler(engine);

      final GenericFutureListener<Future<? super Channel>> listener =
          configuration
              .getSslConfig()
              .getHostNameVerifier(result, configuration.getHost(), context.getThreadId(), engine);

      sslHandler.handshakeFuture().addListener(listener);
      // send SSL request in clear
      connection.channel().writeAndFlush(sslRequest);

      // add SSL handler
      connection.addHandlerFirst(sslHandler);
      return Mono.fromFuture(result);

    } catch (SSLException | R2dbcTransientResourceException e) {
      result.completeExceptionally(e);
      return Mono.fromFuture(result);
    }
  }

  public Flux<ServerMessage> sendCommand(ClientMessage message, DecoderState initialState) {
    return sendCommand(message, initialState, null);
  }

  public abstract Flux<ServerMessage> sendCommand(
      ClientMessage message, DecoderState initialState, String sql);

  @Override
  public Flux<ServerMessage> receive() {
    return Flux.create(
        sink -> {
          this.responseReceivers.add(new CmdElement(sink, DecoderState.INIT_HANDSHAKE));
        });
  }

  public void setContext(InitialHandshakePacket handshake) {
    this.context =
        new ConnectionContext(
            handshake.getServerVersion(),
            handshake.getThreadId(),
            handshake.getSeed(),
            handshake.getCapabilities(),
            handshake.getServerStatus(),
            handshake.isMariaDBServer());
    mariadbPacketDecoder.setContext(context);
    mariadbPacketEncoder.setContext(context);
  }

  public LockAction getLockAction() {
    return new LockAction();
  }

  /**
   * Get current server autocommit.
   *
   * @return autocommit current server value.
   */
  @Override
  public boolean isAutoCommit() {
    return (this.context.getServerStatus() & ServerStatus.AUTOCOMMIT) > 0;
  }

  @Override
  public boolean noBackslashEscapes() {
    return (this.context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) > 0;
  }

  @Override
  public ServerVersion getVersion() {
    return (this.context != null) ? this.context.getVersion() : ServerVersion.UNKNOWN_VERSION;
  }

  @Override
  public boolean isConnected() {
    if (this.isClosed.get()) {
      return false;
    }

    Channel channel = this.connection.channel();
    return channel.isOpen();
  }

  private void closedServlet() {
    if (this.isClosed.compareAndSet(false, true)) {
      clearWaitingListWithError(
          new R2dbcNonTransientResourceException("Connection unexpectedly closed"));

    } else {
      clearWaitingListWithError(new R2dbcNonTransientResourceException("Connection closed"));
    }
  }

  private void clearWaitingListWithError(Throwable exception) {
    CmdElement response;
    while ((response = this.responseReceivers.poll()) != null) {
      response.getSink().error(exception);
    }
  }

  public abstract void sendNext();

  public PrepareCache getPrepareCache() {
    return prepareCache;
  }

  @Override
  public String toString() {
    return "Client{isClosed=" + isClosed + ", context=" + context + '}';
  }

  @SuppressWarnings("serial")
  static class MariadbConnectionException extends R2dbcNonTransientResourceException {
    public MariadbConnectionException(Throwable cause) {
      super(cause);
    }
  }

  public class LockAction implements AutoCloseable {
    public LockAction() {
      lock.lock();
    }

    public Mono<Void> rollbackTransaction() {
      if (!responseReceivers.isEmpty()
          || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        return exchange("ROLLBACK").then();
      } else {
        logger.debug("Skipping savepoint release because no active transaction");
        return Mono.empty();
      }
    }

    public Mono<Void> releaseSavepoint(String name) {
      if (!responseReceivers.isEmpty()
          || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        return exchange(String.format("RELEASE SAVEPOINT `%s`", name.replace("`", "``"))).then();
      } else {
        logger.debug("Skipping savepoint release because no active transaction");
        return Mono.empty();
      }
    }

    public Mono<Void> beginTransaction() {
      if (!responseReceivers.isEmpty()
          || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) == 0) {
        return exchange("BEGIN").then();
      } else {
        logger.debug("Skipping begin transaction because already in transaction");
        return Mono.empty();
      }
    }

    public Mono<Void> commitTransaction() {
      if (!responseReceivers.isEmpty()
          || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        return exchange("COMMIT").then();
      } else {
        logger.debug("Skipping commit transaction because no active transaction");
        return Mono.empty();
      }
    }

    private Flux<ServerMessage> exchange(String sql) {
      ExceptionFactory exceptionFactory = ExceptionFactory.withSql(sql);
      return sendCommand(new QueryPacket(sql)).handle(exceptionFactory::handleErrorResponse);
    }

    public Mono<Void> createSavepoint(String name) {
      if (!responseReceivers.isEmpty()
          || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        return exchange(String.format("SAVEPOINT `%s`", name.replace("`", "``"))).then();
      } else {
        logger.debug("Skipping savepoint creation because no active transaction");
        return Mono.empty();
      }
    }

    public Mono<Void> rollbackTransactionToSavepoint(String name) {
      if (!responseReceivers.isEmpty()
          || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        return exchange(String.format("ROLLBACK TO SAVEPOINT `%s`", name.replace("`", "``")))
            .then();
      } else {
        logger.debug("Skipping rollback to savepoint: no active transaction");
        return Mono.empty();
      }
    }

    public Mono<Void> setAutoCommit(boolean autoCommit) {
      if (!responseReceivers.isEmpty() || autoCommit != isAutoCommit()) {
        return exchange("SET autocommit=" + (autoCommit ? '1' : '0')).then();
      }
      return Mono.empty();
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }
}

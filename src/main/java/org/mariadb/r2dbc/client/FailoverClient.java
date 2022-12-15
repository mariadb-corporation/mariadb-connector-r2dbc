// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import io.r2dbc.spi.TransactionDefinition;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.HaMode;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.*;
import org.mariadb.r2dbc.message.server.CompletePrepareResult;
import org.mariadb.r2dbc.message.server.ErrorPacket;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;
import org.mariadb.r2dbc.message.server.RowPacket;
import org.mariadb.r2dbc.util.HostAddress;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import org.mariadb.r2dbc.util.constants.Capabilities;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

public class FailoverClient implements Client {

  private static final Predicate<? super Throwable> FAIL_PREDICATE =
      R2dbcNonTransientException.class::isInstance;

  private final AtomicReference<Client> client = new AtomicReference<>();
  private final MariadbConnectionConfiguration conf;
  private final ReentrantLock lock;

  private static final Mono<Boolean> reconnectIfNeeded(
      MariadbConnectionConfiguration conf, ReentrantLock lock, AtomicReference<Client> client) {
    if (client.get().isConnected()) return Mono.just(Boolean.TRUE);
    return reconnectFallbackReplay(null, conf, lock, client, true, false, null)
        .then(Mono.just(Boolean.TRUE));
  }

  private static final Mono<ServerMessage> reconnectFallback(
      Throwable t,
      MariadbConnectionConfiguration conf,
      ReentrantLock lock,
      AtomicReference<Client> client) {
    HaMode.failHost(client.get().getHostAddress());
    return conf.getHaMode()
        .connectHost(conf, lock, false)
        .flatMap(
            c ->
                syncNewState(client.get(), c, conf)
                    .flatMap(
                        v -> {
                          client.set(c);
                          return Mono.error(
                              new R2dbcTransientResourceException(
                                  String.format(
                                      "Driver has reconnect connection after a communications link failure with %s. In progress transaction was lost",
                                      client.get().getHostAddress()),
                                  "25S03"));
                        }));
  }

  private static final Mono<Client> reconnectFallbackReplay(
      Throwable throwable,
      MariadbConnectionConfiguration conf,
      ReentrantLock lock,
      AtomicReference<Client> client,
      boolean canSafelyBeReExecuted,
      boolean firstMsgReceived,
      ClientMessage request) {
    HaMode.failHost(client.get().getHostAddress());
    return conf.getHaMode()
        .connectHost(conf, lock, false)
        .onErrorMap(
            t ->
                new R2dbcTransientResourceException(
                    String.format(
                        "Communications link failure with %s, failing to recreate new connection",
                        client.get().getHostAddress()),
                    "25S03",
                    t))
        .flatMap(
            c -> {
              Client oldcli = client.get();
              client.set(c);
              return syncNewState(oldcli, c, conf)
                  .then(
                      replayIfPossible(
                          throwable,
                          oldcli,
                          c,
                          conf,
                          canSafelyBeReExecuted,
                          firstMsgReceived,
                          request))
                  .thenReturn(c);
            });
  }

  public FailoverClient(MariadbConnectionConfiguration conf, ReentrantLock lock, Client client) {
    this.client.set(client);
    this.conf = conf;
    this.lock = lock;
  }

  private static Mono<Void> syncNewState(
      Client oldCli, Client currentClient, MariadbConnectionConfiguration conf) {
    Context oldCtx = oldCli.getContext();

    // sync database
    Mono<Void> monoDatabase;
    if ((oldCtx.getClientCapabilities() | Capabilities.CLIENT_SESSION_TRACK) > 0
        && oldCtx.getDatabase() != null
        && oldCtx.getDatabase().equals(conf.getDatabase())) {
      monoDatabase = Mono.empty();
    } else {
      ExceptionFactory exceptionFactory = ExceptionFactory.withSql("COM_INIT_DB");
      monoDatabase =
          currentClient
              .sendCommand(new ChangeSchemaPacket(oldCtx.getDatabase()), true)
              .handle(exceptionFactory::handleErrorResponse)
              .then();
    }

    // sync transaction isolation
    Mono<Void> monoIsolationLevel;
    if (currentClient.getContext().getIsolationLevel() == oldCtx.getIsolationLevel()) {
      monoIsolationLevel = Mono.empty();
    } else {
      String sql =
          String.format(
              "SET SESSION TRANSACTION ISOLATION LEVEL %s", oldCtx.getIsolationLevel().asSql());
      ExceptionFactory exceptionFactory = ExceptionFactory.withSql(sql);
      monoIsolationLevel =
          currentClient
              .sendCommand(new QueryPacket(sql), true)
              .handle(exceptionFactory::handleErrorResponse)
              .then();
    }

    // sync autoCommit
    return currentClient
        .setAutoCommit(oldCli.isAutoCommit())
        .then(monoDatabase)
        .then(monoIsolationLevel)
        .then();
  }

  private static Mono<Void> replayIfPossible(
      Throwable throwable,
      Client oldClient,
      Client client,
      MariadbConnectionConfiguration conf,
      boolean canRedo,
      boolean firstMsgReceived,
      ClientMessage request) {
    if ((oldClient.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
      if (conf.isTransactionReplay()) {
        if (firstMsgReceived) {
          return Mono.error(
              new R2dbcTransientResourceException(
                  String.format(
                      "Driver has reconnect connection after a communications link failure with %s during command.",
                      oldClient.getHostAddress()),
                  "25S03",
                  throwable));
        }

        return executeTransactionReplay(oldClient, client, request);
      } else {
        // transaction is lost, but connection is now up again.
        // changing exception to SQLTransientConnectionException
        return Mono.error(
            new R2dbcTransientResourceException(
                String.format(
                    "Driver has reconnect connection after a communications link failure with %s. In progress transaction was lost",
                    oldClient.getHostAddress()),
                "25S03",
                throwable));
      }
    }
    return canRedo
        ? Mono.empty()
        : Mono.error(
            new R2dbcTransientResourceException(
                String.format(
                    "Driver has reconnect connection after a communications link failure with %s",
                    oldClient.getHostAddress()),
                "25S03",
                throwable));
  }

  private static Mono<Void> executeTransactionReplay(
      Client oldCli, Client client, ClientMessage request) {
    // transaction replay
    RedoContext ctx = (RedoContext) oldCli.getContext();
    if (ctx.getTransactionSaver().isDirty()) {
      ctx.getTransactionSaver().clear();
      return Mono.error(
          new R2dbcTransientResourceException(
              String.format(
                  "Driver has reconnect connection after a communications link failure with %s. In progress transaction was too big to be replayed, and was lost",
                  oldCli.getHostAddress()),
              "25S03"));
    }
    TransactionSaver transactionSaver = ctx.getTransactionSaver();

    Queue<ClientMessage> endedCmdQueue = transactionSaver.getMessages();
    if (endedCmdQueue.isEmpty()) return Mono.empty();
    transactionSaver.forceDirty();
    Sinks.Many<ClientMessage> cmdSink = Sinks.many().unicast().onBackpressureBuffer();
    AtomicBoolean canceled = new AtomicBoolean();
    return cmdSink
        .asFlux()
        .map(
            it -> {
              it.resetSequencer();
              if (it instanceof PreparePacket) {
                return client
                    .sendCommand(it, DecoderState.PREPARE_RESPONSE, false)
                    .doOnComplete(() -> tryNextCommand(endedCmdQueue, cmdSink, canceled, request));
              } else if (it instanceof ExecutePacket) {
                // command is a prepare statement query
                // redo on new connection need to re-prepare query
                // and substitute statement id
                Mono<ClientMessage> req = ((ExecutePacket) it).rePrepare(client);
                return req.flatMapMany(
                    req2 ->
                        client
                            .sendCommand(req2, false)
                            .doOnComplete(
                                () -> tryNextCommand(endedCmdQueue, cmdSink, canceled, request)));
              } else {
                return client
                    .sendCommand(it, false)
                    .doOnComplete(() -> tryNextCommand(endedCmdQueue, cmdSink, canceled, request));
              }
            })
        .flatMap(mariadbResultFlux -> mariadbResultFlux)
        .doOnCancel(() -> canceled.set(true))
        .doOnDiscard(RowPacket.class, RowPacket::release)
        .doOnError(e -> canceled.set(true))
        .doOnSubscribe(it -> tryNextCommand(endedCmdQueue, cmdSink, canceled, request))
        .onErrorMap(
            e -> new R2dbcTransientResourceException("Socket error during transaction replay", e))
        .doOnComplete(
            () -> {
              ctx.getTransactionSaver().clear();
              ctx.getTransactionSaver().forceDirty();
            })
        .then();
  }

  private static void tryNextCommand(
      Queue<ClientMessage> endedCmdQueue,
      Sinks.Many<ClientMessage> cmdSink,
      AtomicBoolean canceled,
      ClientMessage request) {

    if (canceled.get()) {
      return;
    }

    try {
      ClientMessage endedCmd = endedCmdQueue.poll();
      if (endedCmd != null && (request == null || !request.equals(endedCmd))) {
        cmdSink.emitNext(endedCmd, Sinks.EmitFailureHandler.FAIL_FAST);
      } else {
        cmdSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
      }
    } catch (Exception e) {
      cmdSink.emitError(e, Sinks.EmitFailureHandler.FAIL_FAST);
    }
  }

  @Override
  public Mono<Void> close() {
    return client.get().close();
  }

  @Override
  public boolean closeChannelIfNeeded() {
    return client.get().closeChannelIfNeeded();
  }

  @Override
  public void handleConnectionError(Throwable throwable) {
    client.get().handleConnectionError(throwable);
  }

  @Override
  public void sendCommandWithoutResult(ClientMessage requests) {
    client.get().sendCommandWithoutResult(requests);
  }

  @Override
  public Flux<ServerMessage> sendCommand(ClientMessage requests, boolean canSafelyBeReExecuted) {
    return sendCommand(requests, DecoderState.QUERY_RESPONSE, null, canSafelyBeReExecuted);
  }

  @Override
  public Flux<ServerMessage> sendCommand(
      ClientMessage requests, DecoderState initialState, boolean canSafelyBeReExecuted) {
    return sendCommand(requests, initialState, null, canSafelyBeReExecuted);
  }

  @Override
  public Flux<ServerMessage> sendCommand(
      ClientMessage requests,
      DecoderState initialState,
      String sql,
      boolean canSafelyBeReExecuted) {
    AtomicBoolean firstMsgReceived = new AtomicBoolean(false);
    return reconnectIfNeeded(conf, lock, client)
        .flatMapMany(
            reconnected -> {
              Mono<ClientMessage> clientMsg;
              if (reconnected && requests instanceof ExecutePacket) {
                // in case reconnection occurs during an ExecutePacket, need to re-prepare
                clientMsg = ((ExecutePacket) requests).rePrepare(client.get());
              } else {
                clientMsg = Mono.just(requests);
              }
              return clientMsg.flatMapMany(
                  req ->
                      client
                          .get()
                          .sendCommand(req, initialState, sql, canSafelyBeReExecuted)
                          .switchOnFirst(
                              (signal, serverMessageFlux) -> {
                                // Redo can only be done if subscriber has not began to receive data
                                // for UPSERT command would be ok in a transaction,
                                // but resulting operation would be wrong, having already handle
                                // some
                                // serverMessage.
                                // so all commands that fails before completion, and after receiving
                                // first
                                // message
                                // mustn't be replayed
                                if (signal.getType() == SignalType.ON_NEXT)
                                  firstMsgReceived.set(true);
                                return serverMessageFlux;
                              })
                          .onErrorResume(
                              FAIL_PREDICATE,
                              t ->
                                  reconnectFallbackReplay(
                                          t,
                                          conf,
                                          lock,
                                          client,
                                          canSafelyBeReExecuted,
                                          firstMsgReceived.get(),
                                          req)
                                      .map(
                                          c -> {
                                            req.resetSequencer();
                                            Mono<ClientMessage> clientMsg2;
                                            if (reconnected && req instanceof ExecutePacket) {
                                              // in case reconnection occurs during an
                                              // ExecutePacket, need to re-prepare
                                              clientMsg2 =
                                                  ((ExecutePacket) req).rePrepare(client.get());
                                            } else {
                                              clientMsg2 = Mono.just(req);
                                            }
                                            return clientMsg2.flatMapMany(
                                                req2 ->
                                                    c.sendCommand(
                                                            req2,
                                                            initialState,
                                                            sql,
                                                            canSafelyBeReExecuted)
                                                        .doOnTerminate(() -> req2.releaseSave()));
                                          })
                                      .flatMapMany(flux -> flux)));
            });
  }

  public Mono<ServerPrepareResult> sendPrepare(
      ClientMessage requests, ExceptionFactory factory, String sql) {
    return this.sendCommand(requests, DecoderState.PREPARE_RESPONSE, sql, true)
        .handle(
            (it, sink) -> {
              if (it instanceof ErrorPacket) {
                sink.error(factory.from((ErrorPacket) it));
                return;
              }
              if (it instanceof CompletePrepareResult) {
                sink.next(((CompletePrepareResult) it).getPrepare());
              }
              if (it.ending()) {
                sink.complete();
              }
            })
        .cast(ServerPrepareResult.class)
        .singleOrEmpty();
  }

  @Override
  public Flux<ServerMessage> sendCommand(
      PreparePacket preparePacket, ExecutePacket executePacket, boolean canSafelyBeReExecuted) {
    AtomicBoolean firstMsgReceived = new AtomicBoolean(false);
    return reconnectIfNeeded(conf, lock, client)
        .flatMapMany(
            cc ->
                client
                    .get()
                    .sendCommand(preparePacket, executePacket, canSafelyBeReExecuted)
                    .switchOnFirst(
                        (signal, serverMessageFlux) -> {
                          // Redo can only be done if subscriber has not begun to receive data
                          // for UPSERT command would be ok in a transaction,
                          // but resulting operation would be wrong, having already handle some
                          // serverMessage.
                          // so all commands that fails before completion, and after receiving first
                          // message
                          // mustn't be replayed
                          if (signal.getType() == SignalType.ON_NEXT) firstMsgReceived.set(true);
                          return serverMessageFlux;
                        })
                    .onErrorResume(
                        FAIL_PREDICATE,
                        t ->
                            reconnectFallbackReplay(
                                    t,
                                    conf,
                                    lock,
                                    client,
                                    canSafelyBeReExecuted,
                                    firstMsgReceived.get(),
                                    executePacket)
                                .map(
                                    c -> {
                                      preparePacket.resetSequencer();
                                      executePacket.resetSequencer();
                                      return c.sendCommand(
                                              preparePacket, executePacket, canSafelyBeReExecuted)
                                          .doOnTerminate(() -> executePacket.releaseSave());
                                    })
                                .flatMapMany(flux -> flux)));
  }

  @Override
  public Mono<Void> sendSslRequest(
      SslRequestPacket sslRequest, MariadbConnectionConfiguration configuration) {
    return client.get().sendSslRequest(sslRequest, configuration);
  }

  @Override
  public boolean isAutoCommit() {
    return client.get().isAutoCommit();
  }

  @Override
  public boolean isInTransaction() {
    return client.get().isInTransaction();
  }

  @Override
  public boolean noBackslashEscapes() {
    return client.get().noBackslashEscapes();
  }

  @Override
  public ServerVersion getVersion() {
    return client.get().getVersion();
  }

  @Override
  public boolean isConnected() {
    return client.get().isConnected();
  }

  @Override
  public boolean isCloseRequested() {
    return client.get().isCloseRequested();
  }

  @Override
  public void setContext(InitialHandshakePacket packet, long clientCapabilities) {
    client.get().setContext(packet, clientCapabilities);
  }

  @Override
  public Context getContext() {
    return client.get().getContext();
  }

  @Override
  public PrepareCache getPrepareCache() {
    return client.get().getPrepareCache();
  }

  @Override
  public Mono<Void> beginTransaction() {
    return reconnectIfNeeded(conf, lock, client)
        .flatMap(
            cc ->
                client
                    .get()
                    .beginTransaction()
                    .onErrorResume(
                        FAIL_PREDICATE,
                        t ->
                            reconnectFallbackReplay(t, conf, lock, client, true, false, null)
                                .map(c -> c.beginTransaction())
                                .flatMap(flux -> flux)));
  }

  @Override
  public Mono<Void> beginTransaction(TransactionDefinition definition) {
    return reconnectIfNeeded(conf, lock, client)
        .flatMap(
            cc ->
                client
                    .get()
                    .beginTransaction(definition)
                    .onErrorResume(
                        FAIL_PREDICATE,
                        t ->
                            reconnectFallbackReplay(t, conf, lock, client, true, true, null)
                                .map(c -> c.beginTransaction(definition))
                                .flatMap(flux -> flux)));
  }

  @Override
  public Mono<Void> commitTransaction() {
    // just reconnect
    return client
        .get()
        .commitTransaction()
        .doOnError(FAIL_PREDICATE, t -> reconnectFallback(t, conf, lock, client));
  }

  @Override
  public Mono<Void> rollbackTransaction() {
    return reconnectIfNeeded(conf, lock, client)
        .flatMap(
            cc ->
                client
                    .get()
                    .rollbackTransaction()
                    .onErrorResume(
                        FAIL_PREDICATE,
                        t ->
                            reconnectFallbackReplay(t, conf, lock, client, true, true, null)
                                .map(c -> c.rollbackTransaction())
                                .flatMap(flux -> flux)));
  }

  @Override
  public Mono<Void> setAutoCommit(boolean autoCommit) {
    // setting autocommit to true will commit existing transaction, so if failing we cannot knows if
    // was really committed
    if (autoCommit) {
      return client
          .get()
          .setAutoCommit(true)
          .doOnError(FAIL_PREDICATE, t -> reconnectFallback(t, conf, lock, client));
    }
    return reconnectIfNeeded(conf, lock, client)
        .flatMap(
            cc ->
                client
                    .get()
                    .setAutoCommit(false)
                    .onErrorResume(
                        FAIL_PREDICATE,
                        t ->
                            reconnectFallbackReplay(t, conf, lock, client, true, true, null)
                                .map(c -> c.setAutoCommit(false))
                                .flatMap(flux -> flux)));
  }

  @Override
  public Mono<Void> rollbackTransactionToSavepoint(String name) {
    return client
        .get()
        .rollbackTransactionToSavepoint(name)
        .onErrorResume(
            FAIL_PREDICATE,
            t ->
                reconnectFallbackReplay(t, conf, lock, client, true, true, null)
                    .map(c -> c.rollbackTransactionToSavepoint(name))
                    .flatMap(flux -> flux));
  }

  @Override
  public long getThreadId() {
    return client.get().getThreadId();
  }

  @Override
  public HostAddress getHostAddress() {
    return client.get().getHostAddress();
  }
}

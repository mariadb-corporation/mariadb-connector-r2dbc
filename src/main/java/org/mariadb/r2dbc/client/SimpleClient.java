// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import io.r2dbc.spi.TransactionDefinition;
import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.*;
import org.mariadb.r2dbc.message.server.CompletePrepareResult;
import org.mariadb.r2dbc.message.server.ErrorPacket;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;
import org.mariadb.r2dbc.util.HostAddress;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.*;
import reactor.netty.Connection;
import reactor.netty.channel.AbortedException;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;

public class SimpleClient implements Client {

  private static final Logger logger = Loggers.getLogger(SimpleClient.class);
  protected final MariadbConnectionConfiguration configuration;
  private final ServerMessageSubscriber messageSubscriber;
  private final Sinks.Many<ClientMessage> requestSink =
      Sinks.many().unicast().onBackpressureBuffer();
  private final Queue<Exchange> exchangeQueue =
      Queues.<Exchange>get(Queues.SMALL_BUFFER_SIZE).get();
  private final Queue<ServerMessage> receiverQueue =
      Queues.<ServerMessage>get(Queues.SMALL_BUFFER_SIZE).get();

  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final MariadbFrameDecoder decoder;
  private final MariadbPacketEncoder encoder;
  private final PrepareCache prepareCache;
  private final ByteBufAllocator byteBufAllocator;

  private volatile boolean closeRequested = false;

  protected final ReentrantLock lock;
  protected final Connection connection;
  protected final HostAddress hostAddress;
  protected volatile Context context;

  @Override
  public Context getContext() {
    return context;
  }

  protected SimpleClient(
      Connection connection,
      MariadbConnectionConfiguration configuration,
      HostAddress hostAddress,
      ReentrantLock lock) {
    this.connection = connection;
    this.configuration = configuration;
    this.hostAddress = hostAddress;
    this.lock = lock;
    this.prepareCache =
        new PrepareCache(
            this.configuration.useServerPrepStmts() ? this.configuration.getPrepareCacheSize() : 0,
            this);
    this.decoder = new MariadbFrameDecoder(exchangeQueue, this, configuration);
    this.encoder = new MariadbPacketEncoder();
    this.byteBufAllocator = connection.outbound().alloc();
    this.messageSubscriber = new ServerMessageSubscriber(this.lock, exchangeQueue, receiverQueue);
    connection.addHandlerFirst(this.decoder);

    if (logger.isTraceEnabled()) {
      connection.addHandlerFirst(
          LoggingHandler.class.getSimpleName(),
          new LoggingHandler(SimpleClient.class, LogLevel.TRACE));
    }

    connection
        .inbound()
        .receiveObject()
        .cast(ServerMessage.class)
        .onErrorResume(this::receiveResumeError)
        .subscribe(messageSubscriber);

    this.requestSink
        .asFlux()
        .map(encoder::encodeFlux)
        .flatMap(b -> connection.outbound().send(b), 1)
        .onErrorResume(this::sendResumeError)
        .doAfterTerminate(this::closeChannelIfNeeded)
        .subscribe();
  }

  public static Mono<SimpleClient> connect(
      ConnectionProvider connectionProvider,
      SocketAddress socketAddress,
      HostAddress hostAddress,
      MariadbConnectionConfiguration configuration,
      ReentrantLock lock) {
    TcpClient tcpClient =
        TcpClient.create(connectionProvider)
            .remoteAddress(() -> socketAddress)
            .runOn(configuration.loopResources());
    tcpClient = setSocketOption(configuration, tcpClient);
    return tcpClient
        .connect()
        .flatMap(it -> Mono.just(new SimpleClient(it, configuration, hostAddress, lock)));
  }

  public static TcpClient setSocketOption(
      MariadbConnectionConfiguration configuration, TcpClient tcpClient) {
    if (configuration.getConnectTimeout() != null) {
      tcpClient =
          tcpClient.option(
              ChannelOption.CONNECT_TIMEOUT_MILLIS,
              Math.toIntExact(configuration.getConnectTimeout().toMillis()));
    }

    if (configuration.isTcpKeepAlive()) {
      tcpClient = tcpClient.option(ChannelOption.SO_KEEPALIVE, configuration.isTcpKeepAlive());
    }

    if (configuration.isTcpAbortiveClose()) {
      tcpClient = tcpClient.option(ChannelOption.SO_LINGER, 0);
    }
    return tcpClient;
  }

  public void handleConnectionError(Throwable throwable) {
    if (AbortedException.isConnectionReset(throwable) && !isConnected()) {
      this.messageSubscriber.close(
          new R2dbcNonTransientResourceException(
              "Cannot execute command since connection is already closed", "08000", throwable));
    } else {
      R2dbcNonTransientResourceException error;
      if (throwable instanceof SSLHandshakeException) {
        error = new R2dbcNonTransientResourceException(throwable.getMessage(), "08000", throwable);
      } else {
        error = new R2dbcNonTransientResourceException("Connection error", "08000", throwable);
      }
      this.messageSubscriber.close(error);
      closeChannelIfNeeded();
    }
  }

  private Mono<Void> sendResumeError(Throwable throwable) {

    handleConnectionError(throwable);
    this.requestSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
    return quitOrClose();
  }

  private Mono<ServerMessage> receiveResumeError(Throwable throwable) {
    Mono<ServerMessage> empty = Mono.empty();
    return sendResumeError(throwable).then(empty);
  }

  public boolean closeChannelIfNeeded() {
    if (this.isClosed.compareAndSet(false, true)) {
      Channel channel = this.connection.channel();
      messageSubscriber.close(
          new R2dbcNonTransientResourceException("Connection unexpectedly closed", "08000"));
      if (channel.isOpen()) {
        this.connection.dispose();
      }
      return true;
    }
    return false;
  }

  @Override
  public Mono<Void> close() {
    this.closeRequested = true;
    return quitOrClose();
  }

  private Mono<Void> quitOrClose() {
    return Mono.defer(
        () -> {
          // expect message subscriber to be closed already
          this.messageSubscriber.close(
              new R2dbcNonTransientResourceException(
                  closeRequested ? "Connection has been closed" : "Connection closed", "08000"));

          if (this.isClosed.compareAndSet(false, true)) {
            Channel channel = this.connection.channel();
            if (channel.isOpen()) {
              this.connection.dispose();
              return this.connection.onDispose();
            }

            return Flux.just(QuitPacket.INSTANCE)
                .doOnNext(
                    message ->
                        connection
                            .channel()
                            .writeAndFlush(message.encode(context, context.getByteBufAllocator())))
                .then()
                .doOnSuccess(v -> this.connection.dispose())
                .then(this.connection.onDispose());
          }

          return Mono.empty();
        });
  }

  @Override
  public Mono<Void> sendSslRequest(
      SslRequestPacket sslRequest, MariadbConnectionConfiguration configuration) {
    try {
      SslContext sslContext = configuration.getSslConfig().getSslContext();

      SslHandler sslHandler;
      if (this.hostAddress != null) {
        sslHandler =
            sslContext.newHandler(
                connection.channel().alloc(),
                this.hostAddress.getHost(),
                this.hostAddress.getPort());
        if (configuration.getSslConfig().getSslMode() == SslMode.VERIFY_FULL) {
          SSLEngine sslEngine = sslHandler.engine();
          SSLParameters sslParameters = sslEngine.getSSLParameters();
          sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
          sslEngine.setSSLParameters(sslParameters);
        }
      } else {
        sslHandler = sslContext.newHandler(connection.channel().alloc());
      }

      // send SSL request in clear
      this.requestSink.emitNext(sslRequest, Sinks.EmitFailureHandler.FAIL_FAST);
      // add SSL handler
      connection.addHandlerFirst(sslHandler);
    } catch (Throwable e) {
      this.closeChannelIfNeeded();
      return Mono.error(e);
    }
    return Mono.empty();
  }

  private Flux<ServerMessage> execute(Consumer<FluxSink<ServerMessage>> s) {
    return Flux.create(
        sink -> {
          if (!isConnected()) {
            sink.error(
                new R2dbcNonTransientResourceException(
                    "Connection is close. Cannot send anything"));
            return;
          }
          try {
            lock.lock();
            s.accept(sink);
          } finally {
            lock.unlock();
          }
        });
  }

  public long getThreadId() {
    return context.getThreadId();
  }

  /**
   * Specific implementation, to avoid executing BEGIN if already in transaction
   *
   * @return publisher
   */
  public Mono<Void> beginTransaction() {
    try {
      lock.lock();

      return execute(
              sink -> {
                if (!exchangeQueue.isEmpty()
                    || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) == 0) {
                  Exchange exchange = new Exchange(sink, DecoderState.QUERY_RESPONSE, "BEGIN");
                  if (this.exchangeQueue.offer(exchange)) {
                    this.requestSink.emitNext(
                        new QueryPacket("BEGIN"), Sinks.EmitFailureHandler.FAIL_FAST);
                    sink.onRequest(value -> messageSubscriber.onRequest(exchange, value));
                  } else {
                    sink.error(new R2dbcTransientResourceException("Request queue limit reached"));
                  }
                } else {
                  logger.debug("Skipping start transaction because already in transaction");
                  sink.complete();
                }
              })
          .handle(ExceptionFactory.withSql("BEGIN")::handleErrorResponse)
          .then();

    } finally {
      lock.unlock();
    }
  }

  /**
   * Specific implementation, to avoid executing START TRANSACTION if already in transaction
   *
   * @param definition transaction definition
   * @return publisher
   */
  public Mono<Void> beginTransaction(TransactionDefinition definition) {
    StringBuilder sb = new StringBuilder("START TRANSACTION");
    boolean first = true;
    if (Boolean.TRUE.equals(definition.getAttribute(TransactionDefinition.READ_ONLY))) {
      sb.append(" READ ONLY");
      first = false;
    }
    if (Boolean.TRUE.equals(
        definition.getAttribute(MariadbTransactionDefinition.WITH_CONSISTENT_SNAPSHOT))) {
      if (!first) sb.append(",");
      sb.append(" WITH CONSISTENT SNAPSHOT");
    }

    String sql = sb.toString();
    try {
      lock.lock();
      return execute(
              sink -> {
                if (!exchangeQueue.isEmpty()
                    || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) == 0) {
                  Exchange exchange = new Exchange(sink, DecoderState.QUERY_RESPONSE, sql);
                  if (this.exchangeQueue.offer(exchange)) {
                    this.requestSink.emitNext(
                        new QueryPacket(sql), Sinks.EmitFailureHandler.FAIL_FAST);
                    sink.onRequest(value -> messageSubscriber.onRequest(exchange, value));
                  } else {
                    sink.error(new R2dbcTransientResourceException("Request queue limit reached"));
                  }
                } else {
                  logger.debug("Skipping start transaction because already in transaction");
                  sink.complete();
                }
              })
          .handle(ExceptionFactory.withSql(sql)::handleErrorResponse)
          .then();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Specific implementation, to avoid executing COMMIT if no transaction
   *
   * @return publisher
   */
  public Mono<Void> commitTransaction() {
    try {
      lock.lock();
      return execute(sink -> executeWhenTransaction(sink, "COMMIT"))
          .handle(ExceptionFactory.withSql("COMMIT")::handleErrorResponse)
          .then();
    } finally {
      lock.unlock();
    }
  }

  private void executeWhenTransaction(FluxSink<ServerMessage> sink, String sql) {
    if (!exchangeQueue.isEmpty() || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
      try {
        lock.lock();
        Exchange exchange = new Exchange(sink, DecoderState.QUERY_RESPONSE, sql);
        if (this.exchangeQueue.offer(exchange)) {
          sink.onRequest(value -> messageSubscriber.onRequest(exchange, value));
          this.requestSink.emitNext(new QueryPacket(sql), Sinks.EmitFailureHandler.FAIL_FAST);
        } else {
          sink.error(new R2dbcTransientResourceException("Request queue limit reached"));
        }
      } catch (Throwable t) {
        t.printStackTrace();
        throw t;
      } finally {
        lock.unlock();
      }
    } else {
      logger.debug(String.format("Skipping '%s' because no active transaction", sql));
      sink.complete();
    }
  }

  /**
   * Specific implementation, to avoid executing ROLLBACK if no transaction
   *
   * @return publisher
   */
  public Mono<Void> rollbackTransaction() {
    try {
      lock.lock();
      return execute(sink -> executeWhenTransaction(sink, "ROLLBACK"))
          .handle(ExceptionFactory.withSql("ROLLBACK")::handleErrorResponse)
          .then();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Specific implementation, to avoid executing ROLLBACK TO TRANSACTION if no transaction
   *
   * @return publisher
   */
  public Mono<Void> rollbackTransactionToSavepoint(String name) {
    try {
      lock.lock();
      String sql = String.format("ROLLBACK TO SAVEPOINT `%s`", name.replace("`", "``"));
      return execute(sink -> executeWhenTransaction(sink, sql))
          .handle(ExceptionFactory.withSql(sql)::handleErrorResponse)
          .then();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Specific implementation, to avoid changing autocommit mode if already in this autocommit mode
   *
   * @return publisher
   */
  public Mono<Void> setAutoCommit(boolean autoCommit) {
    try {
      lock.lock();
      return execute(
              sink -> {
                String sql = "SET autocommit=" + (autoCommit ? '1' : '0');
                if (!this.exchangeQueue.isEmpty() || autoCommit != isAutoCommit()) {

                  try {
                    Exchange exchange = new Exchange(sink, DecoderState.QUERY_RESPONSE, sql);
                    if (this.exchangeQueue.offer(exchange)) {
                      sink.onRequest(value -> messageSubscriber.onRequest(exchange, value));
                      this.requestSink.emitNext(
                          new QueryPacket(sql), Sinks.EmitFailureHandler.FAIL_FAST);
                    } else {
                      sink.error(
                          new R2dbcTransientResourceException("Request queue limit reached"));
                    }
                  } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                  }

                } else {
                  logger.debug("Skipping autocommit since already in that state");
                  sink.complete();
                }
              })
          .handle(ExceptionFactory.withSql(null)::handleErrorResponse)
          .then();
    } finally {
      lock.unlock();
    }
  }

  public Flux<ServerMessage> receive(DecoderState initialState) {
    return Flux.create(
        sink -> {
          try {
            lock.lock();
            Exchange exchange = new Exchange(sink, initialState);
            sink.onRequest(value -> messageSubscriber.onRequest(exchange, value));
            if (!this.exchangeQueue.offer(exchange)) {
              sink.error(
                  new R2dbcTransientResourceException(
                      "Request queue limit reached during handshake"));
            }
          } catch (Throwable t) {
            t.printStackTrace();
            throw t;
          } finally {
            lock.unlock();
          }
        });
  }

  public void setContext(InitialHandshakePacket handshake, long clientCapabilities) {
    this.context =
        !HaMode.NONE.equals(configuration.getHaMode()) && configuration.isTransactionReplay()
            ? new RedoContext(
                handshake.getServerVersion(),
                handshake.getThreadId(),
                handshake.getCapabilities(),
                handshake.getServerStatus(),
                handshake.isMariaDBServer(),
                clientCapabilities,
                configuration.getDatabase(),
                byteBufAllocator,
                configuration.getIsolationLevel())
            : new SimpleContext(
                handshake.getServerVersion(),
                handshake.getThreadId(),
                handshake.getCapabilities(),
                handshake.getServerStatus(),
                handshake.isMariaDBServer(),
                clientCapabilities,
                configuration.getDatabase(),
                byteBufAllocator,
                configuration.getIsolationLevel());
    decoder.setContext(context);
    encoder.setContext(context);
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
  public boolean isInTransaction() {
    return (this.context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0;
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
    return this.connection.channel().isOpen();
  }

  @Override
  public boolean isCloseRequested() {
    return this.closeRequested;
  }

  protected class ServerMessageSubscriber implements CoreSubscriber<ServerMessage> {
    private Subscription upstream;
    private volatile boolean close;
    private final AtomicLong receiverDemands = new AtomicLong(0);
    private final ReentrantLock lock;
    private final Queue<Exchange> exchangeQueue;
    private final Queue<ServerMessage> receiverQueue;

    public ServerMessageSubscriber(
        ReentrantLock lock, Queue<Exchange> exchangeQueue, Queue<ServerMessage> receiverQueue) {
      this.lock = lock;
      this.receiverQueue = receiverQueue;
      this.exchangeQueue = exchangeQueue;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      this.upstream = subscription;
    }

    public void onError(Throwable t) {
      t.printStackTrace();
      if (this.close) {
        Operators.onErrorDropped(t, currentContext());
        return;
      }
      requestSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
      SimpleClient.this.handleConnectionError(t);

      // is really needed ?
      SimpleClient.this.quitOrClose().subscribe();
    }

    @Override
    public void onComplete() {
      close(
          new R2dbcNonTransientResourceException(
              String.format(
                  "Connection %s",
                  SimpleClient.this.closeChannelIfNeeded() ? "unexpected error" : "error"),
              "08000"));
      SimpleClient.this.quitOrClose().subscribe();
    }

    @Override
    public void onNext(ServerMessage message) {
      if (this.close) {
        Operators.onNextDropped(message, currentContext());
        return;
      }

      this.receiverDemands.decrementAndGet();
      Exchange exchange = this.exchangeQueue.peek();

      // nothing buffered => directly emit message
      ReferenceCountUtil.retain(message);
      if (this.receiverQueue.isEmpty() && exchange != null && exchange.hasDemand()) {
        if (exchange.emit(message)) this.exchangeQueue.poll();
        if (exchange.hasDemand() || exchange.isCancelled()) {
          requestQueueFilling();
        }
        return;
      }

      // queue message
      if (!this.receiverQueue.offer(message)) {
        message.release();
        Operators.onNextDropped(message, currentContext());
        onError(
            new R2dbcNonTransientResourceException("unexpected : server message queue is full"));
        return;
      }

      tryDrainQueue();
    }

    public void onRequest(Exchange exchange, long n) {
      exchange.incrementDemand(n);
      requestQueueFilling();
      tryDrainQueue();
    }

    private void requestQueueFilling() {
      if (this.receiverQueue.isEmpty()
          && this.receiverDemands.compareAndSet(0, Queues.SMALL_BUFFER_SIZE)) {
        this.upstream.request(Queues.SMALL_BUFFER_SIZE);
      }
    }

    private void tryDrainQueue() {
      Exchange exchange;
      ServerMessage srvMsg;
      while (!this.receiverQueue.isEmpty()) {
        if (!lock.tryLock()) return;
        try {
          while (!this.receiverQueue.isEmpty()) {
            if ((exchange = this.exchangeQueue.peek()) == null || !exchange.hasDemand()) return;
            if ((srvMsg = this.receiverQueue.poll()) == null) return;
            if (exchange.emit(srvMsg)) this.exchangeQueue.poll();
          }
        } finally {
          lock.unlock();
        }

        if ((exchange = this.exchangeQueue.peek()) == null || exchange.hasDemand()) {
          requestQueueFilling();
        }
      }
    }

    public void close(R2dbcException error) {
      this.close = true;
      Exchange exchange;
      while ((exchange = this.exchangeQueue.poll()) != null) {
        exchange.onError(error);
      }
      while (!this.receiverQueue.isEmpty()) {
        this.receiverQueue.poll().release();
      }
    }

    public boolean isClose() {
      return close;
    }
  }

  public void sendCommandWithoutResult(ClientMessage message) {
    try {
      lock.lock();
      this.requestSink.emitNext(message, Sinks.EmitFailureHandler.FAIL_FAST);
    } finally {
      lock.unlock();
    }
  }

  public Flux<ServerMessage> sendCommand(ClientMessage message, boolean canSafelyBeReExecuted) {
    return sendCommand(message, DecoderState.QUERY_RESPONSE, null, canSafelyBeReExecuted);
  }

  public Flux<ServerMessage> sendCommand(
      ClientMessage message, DecoderState initialState, boolean canSafelyBeReExecuted) {
    return sendCommand(message, initialState, null, canSafelyBeReExecuted);
  }

  public Flux<ServerMessage> sendCommand(
      ClientMessage message, DecoderState initialState, String sql, boolean canSafelyBeReExecuted) {
    return Flux.create(
        sink -> {
          if (!isConnected() || messageSubscriber.isClose()) {
            sink.error(
                new R2dbcNonTransientResourceException(
                    "Connection is close. Cannot send anything"));
            return;
          }
          try {
            lock.lock();
            Exchange exchange = new Exchange(sink, initialState, sql);
            if (this.exchangeQueue.offer(exchange)) {
              if (message instanceof PreparePacket) {
                decoder.addPrepare(((PreparePacket) message).getSql());
              }
              sink.onRequest(value -> messageSubscriber.onRequest(exchange, value));
              this.requestSink.emitNext(message, Sinks.EmitFailureHandler.FAIL_FAST);
            } else {
              sink.error(new R2dbcTransientResourceException("Request queue limit reached"));
            }
          } catch (Throwable t) {
            sink.error(t);
          } finally {
            lock.unlock();
          }
        });
  }

  public Mono<ServerPrepareResult> sendPrepare(
      ClientMessage requests, ExceptionFactory factory, String sql) {
    return sendCommand(requests, DecoderState.PREPARE_RESPONSE, sql, true)
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

  public Flux<ServerMessage> sendCommand(
      PreparePacket preparePacket, ExecutePacket executePacket, boolean canSafelyBeReExecuted) {
    return Flux.create(
        sink -> {
          if (!isConnected() || messageSubscriber.isClose()) {
            sink.error(
                new R2dbcNonTransientResourceException(
                    "Connection is close. Cannot send anything"));
            return;
          }
          try {
            lock.lock();
            Exchange exchange =
                new Exchange(
                    sink, DecoderState.PREPARE_AND_EXECUTE_RESPONSE, preparePacket.getSql());
            if (this.exchangeQueue.offer(exchange)) {
              sink.onRequest(value -> messageSubscriber.onRequest(exchange, value));
              decoder.addPrepare(preparePacket.getSql());
              this.requestSink.emitNext(preparePacket, Sinks.EmitFailureHandler.FAIL_FAST);
              this.requestSink.emitNext(executePacket, Sinks.EmitFailureHandler.FAIL_FAST);
            } else {
              sink.error(new R2dbcTransientResourceException("Request queue limit reached"));
            }
          } catch (Throwable t) {
            t.printStackTrace();
            sink.error(t);
          } finally {
            lock.unlock();
          }
        });
  }

  public HostAddress getHostAddress() {
    return hostAddress;
  }

  public PrepareCache getPrepareCache() {
    return prepareCache;
  }

  @Override
  public String toString() {
    return "Client{isClosed=" + isClosed + ", context=" + context + '}';
  }
}

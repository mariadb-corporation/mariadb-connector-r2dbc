// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.r2dbc.spi.*;
import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.*;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;

public class SimpleClient implements Client {

  private static final Logger logger = Loggers.getLogger(SimpleClient.class);
  protected final MariadbConnectionConfiguration configuration;
  private final ServerMessageSubscriber messageSubscriber;
  private final Sinks.Many<Publisher<ClientMessage>> requestSink =
      Sinks.many().unicast().onBackpressureBuffer();
  private final Queue<Exchange> exchangeQueue =
      Queues.<Exchange>get(Queues.SMALL_BUFFER_SIZE).get();
  private final Queue<ServerMessage> receiverQueue =
      Queues.<ServerMessage>get(Queues.SMALL_BUFFER_SIZE).get();

  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final ServerMsgDecoder decoder;
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
    this.decoder = new ServerMsgDecoder(this, configuration);
    this.encoder = new MariadbPacketEncoder();
    this.byteBufAllocator = connection.outbound().alloc();
    this.messageSubscriber =
        new ServerMessageSubscriber(this.lock, this.isClosed, exchangeQueue, receiverQueue);
    connection.addHandlerFirst(new MariadbFrameDecoder());

    if (logger.isTraceEnabled()) {
      connection.addHandlerFirst(
          LoggingHandler.class.getSimpleName(),
          new LoggingHandler(SimpleClient.class, LogLevel.TRACE));
    }

    connection
        .inbound()
        .receive()
        .doOnError(this::handleConnectionError)
        .doOnComplete(this::handleConnectionEnd)
        .subscribe(messageSubscriber);

    Mono<Void> request =
        this.requestSink
            .asFlux()
            .concatMap(Function.identity())
            .cast(ClientMessage.class)
            .map(encoder::encodeFlux)
            .flatMap(b -> connection.outbound().send(Mono.just(b)), 1)
            .then();

    request.doAfterTerminate(this::handleConnectionEnd).subscribe();
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

  private void sendClientMsgs(Publisher<ClientMessage> it) {
    this.requestSink.emitNext(it, Sinks.EmitFailureHandler.FAIL_FAST);
  }

  private void handleConnectionError(Throwable throwable) {
    if (closeChannelIfNeeded()) {
      logger.error("Connection unexpected error", throwable);
      messageSubscriber.endExchanges(
          new R2dbcNonTransientResourceException(
              "Connection unexpected error", "08000", throwable));
    } else {
      logger.error("Connection error", throwable);
      messageSubscriber.endExchanges(
          new R2dbcNonTransientResourceException("Connection error", "08000", throwable));
    }
  }

  private void handleConnectionEnd() {
    messageSubscriber.endExchanges(
        new R2dbcNonTransientResourceException(
            "Connection " + (closeChannelIfNeeded() ? "unexpectedly " : "") + "closed", "08000"));
  }

  private boolean closeChannelIfNeeded() {
    if (this.isClosed.compareAndSet(false, true)) {
      Channel channel = this.connection.channel();
      if (channel.isOpen()) {
        this.connection.dispose();
      }
      return true;
    }
    return false;
  }

  @Override
  public Mono<Void> close() {
    closeRequested = true;
    return Mono.defer(
        () -> {
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

  @Override
  public Mono<Void> sendSslRequest(
      SslRequestPacket sslRequest, MariadbConnectionConfiguration configuration) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    try {
      SslContext sslContext = configuration.getSslConfig().getSslContext();
      SSLEngine engine;
      if (this.hostAddress == null) {
        engine =
            sslContext.newEngine(
                connection.channel().alloc(),
                this.hostAddress.getHost(),
                this.hostAddress.getPort());
      } else {
        engine = sslContext.newEngine(connection.channel().alloc());
      }
      final SslHandler sslHandler = new SslHandler(engine);

      final GenericFutureListener<Future<? super Channel>> listener =
          configuration
              .getSslConfig()
              .getHostNameVerifier(
                  result,
                  this.hostAddress == null ? null : this.hostAddress.getHost(),
                  context.getThreadId(),
                  engine);

      sslHandler.handshakeFuture().addListener(listener);
      // send SSL request in clear
      this.sendClientMsgs(Mono.just(sslRequest));

      // add SSL handler
      connection.addHandlerFirst(sslHandler);
      return Mono.fromFuture(result);

    } catch (SSLException | R2dbcTransientResourceException e) {
      result.completeExceptionally(e);
      return Mono.fromFuture(result);
    }
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
                    sendClientMsgs(Mono.just(new QueryPacket("BEGIN")));
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
                    sendClientMsgs(Mono.just(new QueryPacket(sql)));
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
          sendClientMsgs(Mono.just(new QueryPacket(sql)));
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
                      sendClientMsgs(Mono.just(new QueryPacket(sql)));
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

  protected class ServerMessageSubscriber implements CoreSubscriber<ByteBuf> {
    private Subscription upstream;
    private AtomicBoolean close;
    private final AtomicLong receiverDemands = new AtomicLong(0);
    private final ReentrantLock lock;
    private final Queue<Exchange> exchangeQueue;
    private final Queue<ServerMessage> receiverQueue;

    public ServerMessageSubscriber(
        ReentrantLock lock,
        AtomicBoolean close,
        Queue<Exchange> exchangeQueue,
        Queue<ServerMessage> receiverQueue) {
      this.lock = lock;
      this.close = close;
      this.receiverQueue = receiverQueue;
      this.exchangeQueue = exchangeQueue;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      this.upstream = subscription;
    }

    public void onError(Throwable t) {}

    public void onComplete() {}

    @Override
    public void onNext(ByteBuf message) {
      if (this.close.get()) {
        message.release();
        Operators.onNextDropped(message, currentContext());
        return;
      }

      this.receiverDemands.decrementAndGet();
      Exchange exchange = this.exchangeQueue.peek();
      ServerMessage srvMsg = decoder.decode(message, exchange);

      if (this.receiverQueue.isEmpty() && exchange != null && exchange.hasDemandOrIsCancelled()) {
        // nothing buffered => directly emit message
        if (srvMsg.ending()) this.exchangeQueue.poll();
        exchange.emit(srvMsg);
        if (exchange.hasDemandOrIsCancelled()) {
          requestQueueFilling();
        }
        return;
      }

      // queue message
      if (!this.receiverQueue.offer(srvMsg)) {
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
            if ((exchange = this.exchangeQueue.peek()) == null
                || !exchange.hasDemandOrIsCancelled()) return;
            if ((srvMsg = this.receiverQueue.poll()) == null) return;
            if (srvMsg.ending()) this.exchangeQueue.poll();
            exchange.emit(srvMsg);
          }
        } finally {
          lock.unlock();
        }

        if ((exchange = this.exchangeQueue.peek()) == null || exchange.hasDemandOrIsCancelled()) {
          requestQueueFilling();
        }
      }
    }

    public void endExchanges(Throwable exception) {
      Exchange exchange;
      while ((exchange = this.exchangeQueue.poll()) != null) {
        exchange.getSink().error(exception);
      }
    }
  }

  public void sendCommandWithoutResult(ClientMessage message) {
    try {
      lock.lock();
      sendClientMsgs(Mono.just(message));
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
          if (!isConnected()) {
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
              sendClientMsgs(Mono.just(message));
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
          if (!isConnected()) {
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
              sendClientMsgs(Flux.just(preparePacket, executePacket));
            } else {
              sink.error(new R2dbcTransientResourceException("Request queue limit reached"));
              return;
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

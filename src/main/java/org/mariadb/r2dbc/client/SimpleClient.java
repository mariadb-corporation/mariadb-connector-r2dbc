// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import io.r2dbc.spi.TransactionDefinition;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URLDecoder;
import java.sql.SQLSyntaxErrorException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.Protocol;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.*;
import org.mariadb.r2dbc.message.flow.AuthenticationFlow;
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
  private static final Pattern REDIRECT_URL_FORMAT =
      Pattern.compile(
          "(mariadb|mysql)://(([^/@:]+)?(:([^/]+))?@)?(([^/:]+)(:([0-9]+))?)(/([^?]+)(/?(.*))?)?$",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  protected MariadbConnectionConfiguration configuration;
  protected final ReentrantLock lock;
  protected Connection connection;
  protected HostAddress hostAddress;
  private ServerMessageSubscriber messageSubscriber;
  private Sinks.Many<ClientMessage> requestSink = Sinks.many().unicast().onBackpressureBuffer();
  private Queue<Exchange> exchangeQueue = Queues.<Exchange>get(Queues.SMALL_BUFFER_SIZE).get();
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private MariadbFrameDecoder decoder;
  private MariadbPacketEncoder encoder;
  private final PrepareCache prepareCache;
  private ByteBufAllocator byteBufAllocator;
  protected volatile Context context;
  private volatile boolean closeRequested = false;

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
    Queue<ServerMessage> receiverQueue = Queues.<ServerMessage>get(Queues.SMALL_BUFFER_SIZE).get();
    this.messageSubscriber = new ServerMessageSubscriber(this.lock, exchangeQueue, receiverQueue);
    connection.addHandlerFirst(this.decoder);

    if (configuration.getSslConfig().getSslMode() == SslMode.TUNNEL) {
      SSLEngine engine;
      try {
        SslContext sslContext = configuration.getSslConfig().getSslContext();
        if (hostAddress != null) {
          engine =
              sslContext.newEngine(
                  connection.channel().alloc(), hostAddress.getHost(), hostAddress.getPort());
          SSLParameters sslParameters = engine.getSSLParameters();
          if (!configuration.getSslConfig().sslTunnelDisableHostVerification()) {
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
          }
          engine.setSSLParameters(sslParameters);
        } else {
          engine = sslContext.newEngine(connection.channel().alloc());
        }
        connection.addHandlerFirst(new SslHandler(engine));
      } catch (SSLException e) {
        handleConnectionError(e);
      }
    }

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

  public Mono<Void> redirect() {
    if (configuration.permitRedirect() && context.getRedirectValue() != null) {
      // redirect only if :
      // * when pipelining, having received all waiting responses.
      // * not in a transaction
      if (this.exchangeQueue.size() <= 1
          && (this.context.getServerStatus() & ServerStatus.IN_TRANSACTION) == 0) {
        String redirectValue = context.getRedirectValue();
        context.setRedirect(null);
        // redirection required
        Matcher matcher = REDIRECT_URL_FORMAT.matcher(redirectValue);
        if (!matcher.matches()) {
          return Mono.error(
              new SQLSyntaxErrorException(
                  "error parsing redirection string '"
                      + redirectValue
                      + "'. format must be"
                      + " 'mariadb/mysql://[<user>[:<password>]@]<host>[:<port>]/[<db>[?<opt1>=<value1>[&<opt2>=<value2>]]]'"));
        }

        try {
          String host =
              matcher.group(7) != null
                  ? URLDecoder.decode(matcher.group(7), "UTF-8")
                  : matcher.group(6);
          int port = matcher.group(9) != null ? Integer.parseInt(matcher.group(9)) : 3306;
          HostAddress hostAddress = new HostAddress(host, port);

          // actually only options accepted are user and password
          // there might be additional possible options in the future
          String user = (matcher.group(3) != null) ? matcher.group(3) : configuration.getUsername();
          CharSequence password =
              (matcher.group(5) != null) ? matcher.group(5) : configuration.getPassword();

          MariadbConnectionConfiguration redirectConf =
              configuration.redirectConf(hostAddress, user, password);

          return SimpleClient.connect(
                  ConnectionProvider.newConnection(),
                  InetSocketAddress.createUnresolved(hostAddress.getHost(), hostAddress.getPort()),
                  hostAddress,
                  redirectConf,
                  lock)
              .delayUntil(client -> AuthenticationFlow.exchange(client, redirectConf, hostAddress))
              .doOnError(e -> HaMode.failHost(hostAddress))
              .onErrorComplete()
              .cast(SimpleClient.class)
              .flatMap(
                  client ->
                      MariadbConnectionFactory.setSessionVariables(redirectConf, client)
                          .then(Mono.just(client)))
              .flatMap(this::refreshClient)
              .then();
        } catch (UnsupportedEncodingException e) {
          // "UTF-8" is known, but String decode(String s, Charset charset) requires java 10+ to get
          // rid of catching error
          return Mono.error(e);
        }
      }
    }
    return Mono.empty();
  }

  public Mono<Void> refreshClient(SimpleClient client) {
    return quitOrClose()
        .then(
            Mono.fromCallable(
                () -> {
                  this.isClosed.set(false);
                  this.closeRequested = false;
                  this.connection = client.connection;
                  this.context = client.context;
                  this.configuration = client.configuration;
                  this.hostAddress = client.hostAddress;
                  this.prepareCache.clear();
                  this.requestSink = client.requestSink;
                  this.decoder = client.decoder;
                  this.encoder = client.encoder;
                  this.byteBufAllocator = client.byteBufAllocator;
                  this.messageSubscriber = client.messageSubscriber;
                  this.exchangeQueue = client.exchangeQueue;
                  return Mono.empty();
                }))
        .then();
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
      tcpClient = tcpClient.option(ChannelOption.SO_KEEPALIVE, true);
    }

    if (configuration.isTcpAbortiveClose()) {
      tcpClient = tcpClient.option(ChannelOption.SO_LINGER, 0);
    }
    return tcpClient;
  }

  @Override
  public Context getContext() {
    return context;
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
            if (!channel.isOpen()) {
              this.connection.dispose();
              return this.connection.onDispose();
            }
            return connection
                .outbound()
                .send(encoder.encodeFlux(QuitPacket.INSTANCE))
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

  public long getThreadId() {
    return context.getThreadId();
  }

  /**
   * Specific implementation, to avoid executing BEGIN if already in transaction
   *
   * @return publisher
   */
  public Mono<Void> beginTransaction() {
    return executeWhenNotInTransaction("BEGIN");
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

    return executeWhenNotInTransaction(sb.toString());
  }

  /**
   * Specific implementation, to avoid executing COMMIT if no transaction
   *
   * @return publisher
   */
  public Mono<Void> commitTransaction() {
    return executeWhenTransaction("COMMIT");
  }

  private Mono<Void> executeWhenTransaction(String sql) {
    try {
      lock.lock();
      if (!exchangeQueue.isEmpty()
          || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        Flux<ServerMessage> messages = sendCommand(new QueryPacket(sql), false);
        return messages
            .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
            .handle(ExceptionFactory.withSql(sql)::handleErrorResponse)
            .flatMap(m -> redirect().then(Mono.just(m)))
            .then();
      }
      return Mono.empty();
    } finally {
      lock.unlock();
    }
  }

  private Mono<Void> executeWhenNotInTransaction(String sql) {
    try {
      lock.lock();
      if (!exchangeQueue.isEmpty()
          || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) == 0) {
        Flux<ServerMessage> messages = sendCommand(new QueryPacket(sql), false);
        return messages
            .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
            .handle(ExceptionFactory.withSql(sql)::handleErrorResponse)
            .flatMap(m -> redirect().then(Mono.just(m)))
            .then();
      }
      return Mono.empty();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Specific implementation, to avoid executing ROLLBACK if no transaction
   *
   * @return publisher
   */
  public Mono<Void> rollbackTransaction() {
    return executeWhenTransaction("ROLLBACK");
  }

  /**
   * Specific implementation, to avoid executing ROLLBACK TO TRANSACTION if no transaction
   *
   * @return publisher
   */
  public Mono<Void> rollbackTransactionToSavepoint(String name) {
    String sql = String.format("ROLLBACK TO SAVEPOINT `%s`", name.replace("`", "``"));
    return executeWhenTransaction(sql);
  }

  /**
   * Specific implementation, to avoid changing autocommit mode if already in this autocommit mode
   *
   * @return publisher
   */
  public Mono<Void> setAutoCommit(boolean autoCommit) {

    String sql = "SET autocommit=" + (autoCommit ? '1' : '0');
    try {
      lock.lock();
      if (!this.exchangeQueue.isEmpty() || autoCommit != isAutoCommit()) {
        Flux<ServerMessage> messages = sendCommand(new QueryPacket(sql), false);
        return messages
            .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
            .windowUntil(ServerMessage::resultSetEnd)
            .map(
                dataRow ->
                    new MariadbResult(
                        Protocol.TEXT,
                        null,
                        dataRow,
                        ExceptionFactory.withSql(sql),
                        null,
                        true,
                        configuration))
            .flatMap(m -> redirect().then(Mono.just(m)))
            .cast(org.mariadb.r2dbc.api.MariadbResult.class)
            .then();
      }
      return Mono.empty();
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
            sink.error(t);
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
                    "The connection is closed. Unable to send anything"));
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
                    "The connection is closed. Unable to send anything"));
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

  protected class ServerMessageSubscriber implements CoreSubscriber<ServerMessage> {
    private final AtomicLong receiverDemands = new AtomicLong(0);
    private final ReentrantLock lock;
    private final Queue<Exchange> exchangeQueue;
    private final Queue<ServerMessage> receiverQueue;
    private Subscription upstream;
    private volatile boolean close;

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
      if (!SimpleClient.this.isClosed.get()) {
        SimpleClient.this.quitOrClose().subscribe();
      }
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
}

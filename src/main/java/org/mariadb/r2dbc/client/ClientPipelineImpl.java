// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.ExecutePacket;
import org.mariadb.r2dbc.message.client.PreparePacket;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.util.HostAddress;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.Logger;
import reactor.util.Loggers;

/** Client that send queries pipelining (without waiting for result). */
public final class ClientPipelineImpl extends ClientBase {
  private static final Logger logger = Loggers.getLogger(ClientPipelineImpl.class);

  private ClientPipelineImpl(
      Connection connection,
      MariadbConnectionConfiguration configuration,
      HostAddress hostAddress) {
    super(connection, configuration, hostAddress);
  }

  public static Mono<Client> connect(
      ConnectionProvider connectionProvider,
      SocketAddress socketAddress,
      HostAddress hostAddress,
      MariadbConnectionConfiguration configuration) {
    TcpClient tcpClient =
        TcpClient.create(connectionProvider)
            .remoteAddress(() -> socketAddress)
            .runOn(configuration.loopResources());
    tcpClient = setSocketOption(configuration, tcpClient);
    return tcpClient
        .connect()
        .flatMap(it -> Mono.just(new ClientPipelineImpl(it, configuration, hostAddress)));
  }

  public void sendCommandWithoutResult(ClientMessage message) {
    try {
      lock.lock();
      connection.channel().writeAndFlush(message);
    } finally {
      lock.unlock();
    }
  }

  public Flux<ServerMessage> sendCommand(PreparePacket preparePacket, ExecutePacket executePacket) {
    AtomicBoolean atomicBoolean = new AtomicBoolean();
    return Flux.create(
        sink -> {
          if (!isConnected()) {
            sink.error(
                new R2dbcNonTransientResourceException(
                    "Connection is close. Cannot send anything"));
            return;
          }
          if (atomicBoolean.compareAndSet(false, true)) {
            try {
              lock.lock();
              this.responseReceivers.add(
                  new CmdElement(
                      sink, DecoderState.PREPARE_AND_EXECUTE_RESPONSE, preparePacket.getSql()));
              connection.channel().writeAndFlush(preparePacket);
              connection.channel().writeAndFlush(executePacket);
            } finally {
              lock.unlock();
            }
          }
        });
  }

  public Flux<ServerMessage> sendCommand(
      ClientMessage message, DecoderState initialState, String sql) {
    AtomicBoolean atomicBoolean = new AtomicBoolean();
    return Flux.create(
        sink -> {
          if (!isConnected()) {
            sink.error(
                new R2dbcNonTransientResourceException(
                    "Connection is close. Cannot send anything"));
            return;
          }
          if (atomicBoolean.compareAndSet(false, true)) {
            try {
              lock.lock();
              this.responseReceivers.add(new CmdElement(sink, initialState, sql));
              connection.channel().writeAndFlush(message);
            } finally {
              lock.unlock();
            }
          }
        });
  }

  protected void begin(FluxSink<ServerMessage> sink, String sql) {
    if (!responseReceivers.isEmpty()
        || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) == 0) {
      this.responseReceivers.add(new CmdElement(sink, DecoderState.QUERY_RESPONSE, sql));
      connection.channel().writeAndFlush(new QueryPacket(sql));
    } else {
      logger.debug("Skipping start transaction because already in transaction");
      sink.complete();
    }
  }

  protected void executeAutoCommit(FluxSink<ServerMessage> sink, boolean autoCommit) {
    String cmd = "SET autocommit=" + (autoCommit ? '1' : '0');
    if (this.responseReceivers.isEmpty() || autoCommit != isAutoCommit()) {
      this.responseReceivers.add(new CmdElement(sink, DecoderState.QUERY_RESPONSE, cmd));
      connection.channel().writeAndFlush(new QueryPacket(cmd));
    } else {
      logger.debug("Skipping autocommit since already in that state");
      sink.complete();
    }
  }

  protected void executeWhenTransaction(FluxSink<ServerMessage> sink, String cmd) {
    if (!responseReceivers.isEmpty()
        || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
      this.responseReceivers.add(new CmdElement(sink, DecoderState.QUERY_RESPONSE, cmd));
      connection.channel().writeAndFlush(new QueryPacket(cmd));
    } else {
      logger.debug(String.format("Skipping '%s' because no active transaction", cmd));
      sink.complete();
    }
  }

  public void sendNext() {}
}

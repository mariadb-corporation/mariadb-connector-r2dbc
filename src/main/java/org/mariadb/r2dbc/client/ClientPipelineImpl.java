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

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.client.ExecutePacket;
import org.mariadb.r2dbc.message.client.PreparePacket;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.message.server.ServerMessage;
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

  public ClientPipelineImpl(Connection connection, MariadbConnectionConfiguration configuration) {
    super(connection, configuration);
  }

  public static Mono<Client> connect(
      ConnectionProvider connectionProvider,
      SocketAddress socketAddress,
      MariadbConnectionConfiguration configuration) {

    TcpClient tcpClient = TcpClient.create(connectionProvider).remoteAddress(() -> socketAddress);
    tcpClient = setSocketOption(configuration, tcpClient);
    return tcpClient.connect().flatMap(it -> Mono.just(new ClientPipelineImpl(it, configuration)));
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

  protected void begin(FluxSink<ServerMessage> sink) {
    if (!responseReceivers.isEmpty()
        || (context.getServerStatus() & ServerStatus.IN_TRANSACTION) == 0) {
      this.responseReceivers.add(new CmdElement(sink, DecoderState.QUERY_RESPONSE, "BEGIN"));
      connection.channel().writeAndFlush(new QueryPacket("BEGIN"));
    } else {
      logger.debug("Skipping begin transaction because already in transaction");
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

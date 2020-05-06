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

import io.netty.channel.ChannelOption;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.server.ServerMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.annotation.Nullable;

/** Client that send queries pipelining (without waiting for result). */
public final class ClientPipelineImpl extends ClientBase {
  public ClientPipelineImpl(Connection connection) {
    super(connection);
  }

  public static Mono<Client> connect(
      ConnectionProvider connectionProvider,
      SocketAddress socketAddress,
      @Nullable Duration connectTimeout) {

    TcpClient tcpClient = TcpClient.create(connectionProvider).addressSupplier(() -> socketAddress);
    if (connectTimeout != null) {
      tcpClient =
          tcpClient.option(
              ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.toMillis()));
    }
    return tcpClient.connect().flatMap(it -> Mono.just(new ClientPipelineImpl(it)));
  }

  public Flux<ServerMessage> sendCommand(ClientMessage message, DecoderState initialState) {
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
              this.responseReceivers.add(new CmdElement(sink, initialState));
              connection.channel().writeAndFlush(message);
            } finally {
              lock.unlock();
            }
          }
        });
  }

  public void sendNext() {}
}

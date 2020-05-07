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

import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.client.ExecutePacket;
import org.mariadb.r2dbc.message.client.PreparePacket;
import org.mariadb.r2dbc.message.client.SslRequestPacket;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;
import org.mariadb.r2dbc.message.server.ServerMessage;
import org.mariadb.r2dbc.util.PrepareCache;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface Client {

  Mono<Void> close();

  Flux<ServerMessage> receive();

  void sendCommandWithoutResult(ClientMessage requests);

  Flux<ServerMessage> sendCommand(ClientMessage requests);

  Flux<ServerMessage> sendCommand(ClientMessage requests, DecoderState initialState);

  Flux<ServerMessage> sendCommand(ClientMessage requests, DecoderState initialState, String sql);

  Flux<ServerMessage> sendCommand(PreparePacket preparePacket, ExecutePacket executePacket);

  Mono<Void> sendSslRequest(
      SslRequestPacket sslRequest, MariadbConnectionConfiguration configuration);

  ClientBase.LockAction getLockAction();

  boolean isAutoCommit();

  boolean noBackslashEscapes();

  ServerVersion getVersion();

  boolean isConnected();

  void setContext(InitialHandshakePacket packet);

  void sendNext();

  PrepareCache getPrepareCache();
}

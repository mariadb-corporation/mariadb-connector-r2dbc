// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

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

  Flux<ServerMessage> receive(DecoderState initialState);

  void sendCommandWithoutResult(ClientMessage requests);

  Flux<ServerMessage> sendCommand(ClientMessage requests);

  Flux<ServerMessage> sendCommand(ClientMessage requests, DecoderState initialState);

  Flux<ServerMessage> sendCommand(ClientMessage requests, DecoderState initialState, String sql);

  Flux<ServerMessage> sendCommand(PreparePacket preparePacket, ExecutePacket executePacket);

  Mono<Void> sendSslRequest(
      SslRequestPacket sslRequest, MariadbConnectionConfiguration configuration);

  boolean isAutoCommit();

  boolean noBackslashEscapes();

  ServerVersion getVersion();

  boolean isConnected();

  void setContext(InitialHandshakePacket packet);

  void sendNext();

  MariadbConnectionConfiguration getConf();

  PrepareCache getPrepareCache();

  Mono<Void> beginTransaction();

  Mono<Void> commitTransaction();

  Mono<Void> rollbackTransaction();

  Mono<Void> setAutoCommit(boolean autoCommit);

  Mono<Void> rollbackTransactionToSavepoint(String name);

  Mono<Void> releaseSavepoint(String name);

  Mono<Void> createSavepoint(String name);

  long getThreadId();
}

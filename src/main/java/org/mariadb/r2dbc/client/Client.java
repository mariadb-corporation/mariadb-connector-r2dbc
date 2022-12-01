// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.r2dbc.spi.TransactionDefinition;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.ExecutePacket;
import org.mariadb.r2dbc.message.client.PreparePacket;
import org.mariadb.r2dbc.message.client.SslRequestPacket;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;
import org.mariadb.r2dbc.util.HostAddress;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface Client {

  Mono<Void> close();

  boolean closeChannelIfNeeded();

  void sendCommandWithoutResult(ClientMessage requests);

  Flux<ServerMessage> sendCommand(ClientMessage requests, boolean canSafelyBeReExecuted);

  Flux<ServerMessage> sendCommand(
      ClientMessage requests, DecoderState initialState, boolean canSafelyBeReExecuted);

  Flux<ServerMessage> sendCommand(
      ClientMessage requests, DecoderState initialState, String sql, boolean canSafelyBeReExecuted);

  Flux<ServerMessage> sendCommand(
      PreparePacket preparePacket, ExecutePacket executePacket, boolean canSafelyBeReExecuted);

  Mono<ServerPrepareResult> sendPrepare(
      ClientMessage requests, ExceptionFactory factory, String sql);

  Mono<Void> sendSslRequest(
      SslRequestPacket sslRequest, MariadbConnectionConfiguration configuration);

  boolean isAutoCommit();

  boolean isInTransaction();

  boolean noBackslashEscapes();

  ServerVersion getVersion();

  boolean isConnected();

  boolean isCloseRequested();

  void setContext(InitialHandshakePacket packet, long clientCapabilities);

  Context getContext();

  PrepareCache getPrepareCache();

  Mono<Void> beginTransaction();

  Mono<Void> beginTransaction(TransactionDefinition definition);

  Mono<Void> commitTransaction();

  Mono<Void> rollbackTransaction();

  Mono<Void> setAutoCommit(boolean autoCommit);

  Mono<Void> rollbackTransactionToSavepoint(String name);

  long getThreadId();

  HostAddress getHostAddress();
}

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

package org.mariadb.r2dbc.message.flow;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.SslMode;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.DecoderState;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.client.HandshakeResponse;
import org.mariadb.r2dbc.message.client.SslRequestPacket;
import org.mariadb.r2dbc.message.server.*;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.constants.Capabilities;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class AuthenticationFlow {
  private static final Logger logger = Loggers.getLogger(AuthenticationFlow.class);

  private MariadbConnectionConfiguration configuration;
  private InitialHandshakePacket initialHandshakePacket;
  private AuthenticationPlugin pluginHandler;
  private AuthSwitchPacket authSwitchPacket;
  private AuthMoreDataPacket authMoreDataPacket;
  private Client client;
  private long clientCapabilities;

  private AuthenticationFlow(Client client, MariadbConnectionConfiguration configuration) {
    this.client = client;
    this.configuration = configuration;
  }

  public static Mono<Client> exchange(Client client, MariadbConnectionConfiguration configuration) {
    AuthenticationFlow flow = new AuthenticationFlow(client, configuration);
    Assert.requireNonNull(client, "client must not be null");

    EmitterProcessor<State> stateMachine = EmitterProcessor.create(true);

    return stateMachine
        .startWith(State.INIT)
        .<Void>handle(
            (state, sink) -> {
              if (State.COMPLETED == state) {
                sink.complete();
              } else {
                if (logger.isDebugEnabled()) {
                  logger.debug("authentication state {}", state);
                }
                state.handle(flow).subscribe(stateMachine::onNext, stateMachine::onError);
              }
            })
        .doOnNext(tt -> {})
        .doOnComplete(
            () -> {
              if (logger.isDebugEnabled()) {
                logger.debug("Authentication success");
              }
            })
        .doOnError(
            e -> {
              logger.debug("error", e);
              flow.client.close().subscribe();
            })
        .then(Mono.just(client));
  }

  private static long initializeClientCapabilities(
      final long serverCapabilities, MariadbConnectionConfiguration configuration) {
    long capabilities =
        Capabilities.IGNORE_SPACE
            | Capabilities.CLIENT_PROTOCOL_41
            | Capabilities.TRANSACTIONS
            | Capabilities.SECURE_CONNECTION
            | Capabilities.MULTI_RESULTS
            | Capabilities.PS_MULTI_RESULTS
            | Capabilities.PLUGIN_AUTH
            | Capabilities.CONNECT_ATTRS
            | Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA
            | Capabilities.CLIENT_SESSION_TRACK
            | Capabilities.FOUND_ROWS;

    if (configuration.allowMultiQueries()) {
      capabilities |= Capabilities.MULTI_STATEMENTS;
    }

    if ((serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) != 0) {
      capabilities |= Capabilities.CLIENT_DEPRECATE_EOF;
    }

    if (configuration.getDatabase() != null && !configuration.getDatabase().isEmpty()) {
      capabilities |= Capabilities.CONNECT_WITH_DB;
    }
    return capabilities;
  }

  private HandshakeResponse createHandshakeResponse(long clientCapabilities) {
    return new HandshakeResponse(
        this.initialHandshakePacket,
        this.configuration.getUsername(),
        this.configuration.getPassword(),
        this.configuration.getDatabase(),
        configuration.getConnectionAttributes(),
        configuration.getHost(),
        clientCapabilities);
  }

  private SslRequestPacket createSslRequest(long clientCapabilities) {
    return new SslRequestPacket(this.initialHandshakePacket, clientCapabilities);
  }

  public enum State {
    INIT {
      @Override
      Mono<State> handle(AuthenticationFlow flow) {
        // Server send first, so no need send anything to server in here.
        return flow.client
            .receive()
            .<State>handle(
                (message, sink) -> {
                  if (message instanceof InitialHandshakePacket) {
                    flow.client.setContext((InitialHandshakePacket) message);
                    // TODO SET connection context with server data.
                    InitialHandshakePacket packet = (InitialHandshakePacket) message;
                    flow.initialHandshakePacket = packet;
                    flow.clientCapabilities =
                        initializeClientCapabilities(
                            flow.initialHandshakePacket.getCapabilities(), flow.configuration);

                    if (flow.configuration.getSslConfig().getSslMode() != SslMode.DISABLED) {
                      if ((packet.getCapabilities() & Capabilities.SSL) == 0) {
                        sink.error(
                            new R2dbcNonTransientResourceException(
                                "Trying to connect with ssl, but ssl not enabled in the server",
                                "08000"));
                      } else {
                        sink.next(SSL_REQUEST);
                      }
                    } else {
                      sink.next(HANDSHAKE);
                    }

                  } else {
                    sink.error(
                        new IllegalStateException(
                            String.format(
                                "Unexpected message type '%s' in handshake init phase",
                                message.getClass().getSimpleName())));
                  }
                })
            .next();
      }
    },

    SSL_REQUEST {
      @Override
      Mono<State> handle(AuthenticationFlow flow) {
        flow.clientCapabilities |= Capabilities.SSL;
        SslRequestPacket sslRequest = flow.createSslRequest(flow.clientCapabilities);
        return flow.client
            .sendSslRequest(sslRequest, flow.configuration)
            .then(Mono.just(HANDSHAKE));
      }
    },

    HANDSHAKE {
      @Override
      Mono<State> handle(AuthenticationFlow flow) {
        return flow.client
            .sendCommand(
                flow.createHandshakeResponse(flow.clientCapabilities),
                DecoderState.AUTHENTICATION_SWITCH_RESPONSE)
            .<State>handle(
                (message, sink) -> {
                  if (message instanceof ErrorPacket) {
                    R2dbcException exception =
                        ExceptionFactory.createException((ErrorPacket) message, null);
                    sink.error(
                        new R2dbcNonTransientResourceException(exception.getMessage(), exception));
                  } else if (message instanceof OkPacket) {
                    sink.next(COMPLETED);
                  } else if (message instanceof AuthSwitchPacket) {
                    flow.authSwitchPacket = ((AuthSwitchPacket) message);
                    String plugin = flow.authSwitchPacket.getPlugin();
                    AuthenticationPlugin authPlugin = AuthenticationFlowPluginLoader.get(plugin);
                    flow.authMoreDataPacket = null;
                    flow.pluginHandler = authPlugin;
                    sink.next(AUTH_SWITCH);
                  } else {
                    sink.error(
                        new IllegalStateException(
                            String.format(
                                "Unexpected message type '%s' in handshake response phase",
                                message.getClass().getSimpleName())));
                  }
                })
            .next();
      }
    },

    AUTH_SWITCH {
      @Override
      Mono<State> handle(AuthenticationFlow flow) {
        ClientMessage clientMessage;
        try {
          clientMessage =
              flow.pluginHandler.next(
                  flow.configuration, flow.authSwitchPacket, flow.authMoreDataPacket);
        } catch (R2dbcException ex) {
          return Mono.error(ex);
        }

        Flux<ServerMessage> flux;
        if (clientMessage != null) {
          // this can occur when there is a "finishing" message for authentication plugin
          // example CachingSha2PasswordFlow that finish with a successful FAST_AUTH
          flux =
              flow.client.sendCommand(clientMessage, DecoderState.AUTHENTICATION_SWITCH_RESPONSE);
        } else {
          flux = flow.client.receive();
        }

        return flux.<State>handle(
                (message, sink) -> {
                  if (message instanceof ErrorPacket) {
                    sink.error(
                        new R2dbcNonTransientResourceException(
                            ((ErrorPacket) message).getMessage()));
                  } else if (message instanceof OkPacket) {
                    sink.next(COMPLETED);
                  } else if (message instanceof AuthSwitchPacket) {
                    flow.authSwitchPacket = ((AuthSwitchPacket) message);
                    String plugin = flow.authSwitchPacket.getPlugin();
                    AuthenticationPlugin authPlugin = AuthenticationFlowPluginLoader.get(plugin);
                    flow.authMoreDataPacket = null;
                    flow.pluginHandler = authPlugin;
                    sink.next(AUTH_SWITCH);
                  } else if (message instanceof AuthMoreDataPacket) {
                    flow.authMoreDataPacket = (AuthMoreDataPacket) message;
                    sink.next(AUTH_SWITCH);
                  } else {
                    sink.error(
                        new IllegalStateException(
                            String.format(
                                "Unexpected message type '%s' in handshake response phase",
                                message.getClass().getSimpleName())));
                  }
                })
            .next();
      }
    },

    COMPLETED {
      @Override
      Mono<State> handle(AuthenticationFlow flow) {
        return Mono.just(COMPLETED);
      }
    };

    abstract Mono<State> handle(AuthenticationFlow flow);
  }
}

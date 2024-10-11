// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.flow;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import java.util.Arrays;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.SslMode;
import org.mariadb.r2dbc.authentication.AuthenticationFlowPluginLoader;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.authentication.standard.CachingSha2PasswordFlow;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.DecoderState;
import org.mariadb.r2dbc.client.SimpleClient;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.HandshakeResponse;
import org.mariadb.r2dbc.message.client.SslRequestPacket;
import org.mariadb.r2dbc.message.server.*;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.HostAddress;
import org.mariadb.r2dbc.util.constants.Capabilities;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class AuthenticationFlow {
  private static final Logger logger = Loggers.getLogger(AuthenticationFlow.class);

  private final MariadbConnectionConfiguration configuration;
  private final SimpleClient client;
  private final HostAddress hostAddress;
  private InitialHandshakePacket initialHandshakePacket;
  private AuthenticationPlugin pluginHandler;
  private byte[] seed;
  private Sequencer sequencer;
  private AuthMoreDataPacket authMoreDataPacket;
  private FluxSink<State> sink;
  private long clientCapabilities;

  private AuthenticationFlow(
      SimpleClient client, MariadbConnectionConfiguration configuration, HostAddress hostAddress) {
    this.client = client;
    this.configuration = configuration;
    this.hostAddress = hostAddress;
  }

  public static Mono<Client> exchange(
      SimpleClient client, MariadbConnectionConfiguration configuration, HostAddress hostAddress) {
    AuthenticationFlow flow = new AuthenticationFlow(client, configuration, hostAddress);
    Assert.requireNonNull(client, "client must not be null");

    return Flux.<State>create(
            sink -> {
              flow.sink = sink;
              State.INIT.handle(flow).subscribe(sink::next, sink::error);
            })
        .doOnNext(
            state -> {
              if (State.COMPLETED == state) {
                if (flow.authMoreDataPacket != null) {
                  flow.authMoreDataPacket.release();
                  flow.authMoreDataPacket = null;
                }
                flow.sink.complete();
              } else {
                if (logger.isTraceEnabled()) {
                  logger.trace("authentication state {}", state);
                }
                state.handle(flow).subscribe(flow.sink::next, flow.sink::error);
              }
            })
        .doOnComplete(
            () -> {
              if (logger.isDebugEnabled()) {
                logger.debug("Authentication success");
              }
            })
        .doOnError(
            e -> {
              logger.warn("Authentication failed", e);
              flow.client.handleConnectionError(e);
            })
        .doFinally(
            c -> {
              if (flow.authMoreDataPacket != null) {
                flow.authMoreDataPacket.release();
                flow.authMoreDataPacket = null;
              }
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
            | Capabilities.FOUND_ROWS
            | Capabilities.MARIADB_CLIENT_CACHE_METADATA;

    if (configuration.allowMultiQueries()) {
      capabilities |= Capabilities.MULTI_STATEMENTS;
    }

    if (configuration.getDatabase() != null) {
      capabilities |= Capabilities.CONNECT_WITH_DB;
    }

    return capabilities & serverCapabilities;
  }

  private HandshakeResponse createHandshakeResponse(long clientCapabilities) {
    return new HandshakeResponse(
        this.initialHandshakePacket,
        this.configuration.getUsername(),
        this.configuration.getPassword(),
        this.configuration.getDatabase(),
        configuration.getConnectionAttributes(),
        this.hostAddress,
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
            .receive(DecoderState.INIT_HANDSHAKE)
            .<State>handle(
                (message, sink) -> {
                  if (message instanceof ErrorPacket) {
                    sink.error(ExceptionFactory.INSTANCE.from((ErrorPacket) message));
                  } else if (message instanceof InitialHandshakePacket) {
                    InitialHandshakePacket packet = (InitialHandshakePacket) message;
                    flow.initialHandshakePacket = packet;
                    flow.clientCapabilities =
                        initializeClientCapabilities(
                            flow.initialHandshakePacket.getCapabilities(), flow.configuration);
                    flow.client.setContext(packet, flow.clientCapabilities);

                    SslMode sslMode = flow.configuration.getSslConfig().getSslMode();
                    if (sslMode != SslMode.DISABLE && sslMode != SslMode.TUNNEL) {
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
        flow.seed = flow.initialHandshakePacket.getSeed();
        flow.sequencer = flow.initialHandshakePacket.getSequencer();

        if (flow.initialHandshakePacket
            .getAuthenticationPluginType()
            .equals(CachingSha2PasswordFlow.TYPE)) {
          AuthenticationPlugin authPlugin =
              AuthenticationFlowPluginLoader.get(CachingSha2PasswordFlow.TYPE);
          ((CachingSha2PasswordFlow) authPlugin).setStateFastAuth();
          flow.authMoreDataPacket = null;
          flow.pluginHandler = authPlugin;
        }

        return flow.client
            .sendCommand(
                flow.createHandshakeResponse(flow.clientCapabilities),
                DecoderState.AUTHENTICATION_SWITCH_RESPONSE,
                false)
            .<State>handle(
                (message, sink) -> {
                  if (message instanceof ErrorPacket) {
                    sink.error(ExceptionFactory.createException((ErrorPacket) message, null));
                  } else if (message instanceof OkPacket) {
                    sink.next(COMPLETED);
                  } else if (message instanceof AuthSwitchPacket) {
                    AuthSwitchPacket authSwitchPacket = ((AuthSwitchPacket) message);
                    flow.seed = authSwitchPacket.getSeed();
                    flow.sequencer = authSwitchPacket.getSequencer();
                    String plugin = authSwitchPacket.getPlugin();
                    if (flow.configuration.getRestrictedAuth() != null
                        && !Arrays.stream(flow.configuration.getRestrictedAuth())
                            .anyMatch(s -> plugin.equals(s))) {
                      sink.error(
                          new R2dbcPermissionDeniedException(
                              String.format(
                                  "Unsupported authentication plugin %s. Authorized plugin: %s",
                                  plugin,
                                  Arrays.toString(flow.configuration.getRestrictedAuth()))));
                    } else {
                      AuthenticationPlugin authPlugin = AuthenticationFlowPluginLoader.get(plugin);
                      flow.authMoreDataPacket = null;
                      flow.pluginHandler = authPlugin;
                      sink.next(AUTH_SWITCH);
                    }
                  } else if (flow.pluginHandler != null && message instanceof AuthMoreDataPacket) {
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

    AUTH_SWITCH {
      @Override
      Mono<State> handle(AuthenticationFlow flow) {
        ClientMessage clientMessage;
        try {
          clientMessage =
              flow.pluginHandler.next(
                  flow.configuration, flow.seed, flow.sequencer, flow.authMoreDataPacket);
        } catch (R2dbcException ex) {
          return Mono.error(ex);
        }

        Flux<ServerMessage> flux;
        if (clientMessage != null) {
          // this can occur when there is a "finishing" message for authentication plugin
          // example CachingSha2PasswordFlow that finish with a successful FAST_AUTH
          flux =
              flow.client.sendCommand(
                  clientMessage, DecoderState.AUTHENTICATION_SWITCH_RESPONSE, false);
        } else {
          flux = flow.client.receive(DecoderState.AUTHENTICATION_SWITCH_RESPONSE);
        }
        if (flow.authMoreDataPacket != null) {
          flow.authMoreDataPacket.release();
          flow.authMoreDataPacket = null;
        }
        return flux.<State>handle(
                (message, sink) -> {
                  if (message instanceof ErrorPacket) {
                    sink.error(
                        new R2dbcNonTransientResourceException(((ErrorPacket) message).message()));
                  } else if (message instanceof OkPacket) {
                    sink.next(COMPLETED);
                  } else if (message instanceof AuthSwitchPacket) {
                    AuthSwitchPacket authSwitchPacket = ((AuthSwitchPacket) message);
                    flow.seed = authSwitchPacket.getSeed();
                    flow.sequencer = authSwitchPacket.getSequencer();
                    String plugin = authSwitchPacket.getPlugin();
                    if (flow.configuration.getRestrictedAuth() != null
                        && !Arrays.stream(flow.configuration.getRestrictedAuth())
                            .anyMatch(s -> plugin.equals(s))) {
                      sink.error(
                          new R2dbcPermissionDeniedException(
                              String.format(
                                  "Unsupported authentication plugin %s. Authorized plugin: %s",
                                  plugin,
                                  Arrays.toString(flow.configuration.getRestrictedAuth()))));
                    } else {
                      AuthenticationPlugin authPlugin = AuthenticationFlowPluginLoader.get(plugin);
                      flow.authMoreDataPacket = null;
                      flow.pluginHandler = authPlugin;
                      sink.next(AUTH_SWITCH);
                    }
                  } else if (message instanceof AuthMoreDataPacket) {
                    flow.authMoreDataPacket = (AuthMoreDataPacket) message;
                    flow.sequencer = (Sequencer) ((AuthMoreDataPacket) message).getSequencer();
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

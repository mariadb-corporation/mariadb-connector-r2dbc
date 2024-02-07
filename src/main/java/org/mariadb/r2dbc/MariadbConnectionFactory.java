// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.*;
import java.net.SocketAddress;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.FailoverClient;
import org.mariadb.r2dbc.client.MariadbResult;
import org.mariadb.r2dbc.client.SimpleClient;
import org.mariadb.r2dbc.message.Protocol;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.message.flow.AuthenticationFlow;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.HostAddress;
import org.mariadb.r2dbc.util.constants.Capabilities;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;

public final class MariadbConnectionFactory implements ConnectionFactory {

  private final MariadbConnectionConfiguration configuration;

  public MariadbConnectionFactory(MariadbConnectionConfiguration configuration) {
    this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
  }

  public static MariadbConnectionFactory from(MariadbConnectionConfiguration configuration) {
    return new MariadbConnectionFactory(configuration);
  }

  private static Mono<Client> connectToSocket(
      final MariadbConnectionConfiguration configuration,
      SocketAddress endpoint,
      HostAddress hostAddress,
      ReentrantLock lock) {
    return SimpleClient.connect(
            ConnectionProvider.newConnection(), endpoint, hostAddress, configuration, lock)
        .delayUntil(client -> AuthenticationFlow.exchange(client, configuration, hostAddress))
        .cast(Client.class)
        .flatMap(client -> setSessionVariables(configuration, client).thenReturn(client))
        .onErrorMap(e -> cannotConnect(e, endpoint));
  }

  private static Mono<String> setTimezoneIfNeeded(
      final MariadbConnectionConfiguration configuration, Client client) {
    if (!"disable".equalsIgnoreCase(configuration.getTimezone())) {
      return client
          .sendCommand(new QueryPacket("SELECT @@time_zone, @@system_time_zone"), true)
          .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
          .windowUntil(ServerMessage::resultSetEnd)
          .map(
              dataRow ->
                  new MariadbResult(
                      Protocol.TEXT,
                      null,
                      dataRow,
                      ExceptionFactory.INSTANCE,
                      null,
                      false,
                      configuration))
          .flatMap(
              r ->
                  r.map(
                      (row, metadata) -> {
                        String serverTz = row.get(0, String.class);
                        if ("SYSTEM".equals(serverTz)) {
                          serverTz = row.get(1, String.class);
                        }
                        boolean mustSetTimezone = true;
                        TimeZone connectionTz =
                            "auto".equalsIgnoreCase(configuration.getTimezone())
                                ? TimeZone.getDefault()
                                : TimeZone.getTimeZone(
                                    ZoneId.of(configuration.getTimezone()).normalized());
                        ZoneId clientZoneId = connectionTz.toZoneId();

                        // try to avoid timezone consideration if server use the same one
                        try {
                          assert serverTz != null;
                          ZoneId serverZoneId = ZoneId.of(serverTz);
                          if (serverZoneId.normalized().equals(clientZoneId)
                              || ZoneId.of(serverTz, ZoneId.SHORT_IDS).equals(clientZoneId)) {
                            mustSetTimezone = false;
                          }
                        } catch (DateTimeException e) {
                          // eat
                        }

                        if (mustSetTimezone) {
                          if (clientZoneId.getRules().isFixedOffset()) {
                            ZoneOffset zoneOffset =
                                clientZoneId.getRules().getOffset(Instant.now());
                            if (zoneOffset.getTotalSeconds() == 0) {
                              // specific for UTC timezone, server permitting only SYSTEM/UTC offset
                              // or named time
                              // zone
                              // not 'UTC'/'Z'
                              return ",time_zone='+00:00'";
                            } else {
                              return ",time_zone='" + zoneOffset.getId() + "'";
                            }
                          } else {
                            return ",time_zone='" + clientZoneId.normalized() + "'";
                          }
                        }
                        return "";
                      }))
          .last();
    }
    return Mono.just("");
  }

  public static Mono<Void> setSessionVariables(
      final MariadbConnectionConfiguration configuration, Client client) {

    // set default autocommit value
    StringBuilder sql =
        new StringBuilder("SET autocommit=" + (configuration.autocommit() ? "1" : "0"));

    // set default transaction isolation
    String txIsolation =
        (client.getVersion().isMariaDBServer()
                    && client.getVersion().versionGreaterOrEqual(11, 1, 1))
                || (!client.getVersion().isMariaDBServer()
                    && (client.getVersion().versionGreaterOrEqual(8, 0, 3)
                        || (client.getVersion().getMajorVersion() < 8
                            && client.getVersion().versionGreaterOrEqual(5, 7, 20))))
            ? "transaction_isolation"
            : "tx_isolation";
    sql.append(",")
        .append(txIsolation)
        .append("='")
        .append(
            configuration.getIsolationLevel() == null
                ? "REPEATABLE-READ"
                : configuration.getIsolationLevel().asSql().replace(" ", "-"))
        .append("'");

    // set session tracking
    if ((client.getContext().getClientCapabilities() & Capabilities.CLIENT_SESSION_TRACK) > 0) {
      sql.append(",session_track_schema=1");
      if (!client.getContext().getVersion().isMariaDBServer()) {
        // MySQL only support 8 version that always have autocommit and doesn't permit adding
        // autocommit value if already present
        sql.append(",session_track_system_variables=CONCAT(@@session_track_system_variables,',")
            .append(txIsolation)
            .append("')");
      } else {
        sql.append(
                ",session_track_system_variables=CONCAT(@@session_track_system_variables,',autocommit,")
            .append(txIsolation)
            .append("')");
      }
    }

    // set session variables if defined
    if (configuration.getSessionVariables() != null
        && configuration.getSessionVariables().size() > 0) {
      Map<String, Object> sessionVariable = configuration.getSessionVariables();
      Iterator<String> keys = sessionVariable.keySet().iterator();
      for (int i = 0; i < sessionVariable.size(); i++) {
        String key = keys.next();
        Object value = sessionVariable.get(key);
        if (value == null) {
          client.close().subscribe();
          return Mono.error(
              new R2dbcNonTransientResourceException(
                  String.format("Session variable '%s' has no value", key)));
        }
        sql.append(",").append(key).append("=");
        if (value instanceof String) {
          sql.append("'").append(value).append("'");
        } else if (value instanceof Integer
            || value instanceof Boolean
            || value instanceof Double) {
          sql.append(value);
        } else {
          client.close().subscribe();
          return Mono.error(
              new R2dbcNonTransientResourceException(
                  String.format(
                      "Session variable '%s' type can only be of type String, Integer, Double or"
                          + " Boolean",
                      key)));
        }
      }
    }
    sql.append(", names UTF8MB4");
    if (configuration.getCollation() != null && !configuration.getCollation().isEmpty())
      sql.append(" COLLATE ").append(configuration.getCollation());

    return setTimezoneIfNeeded(configuration, client)
        .map(sql::append)
        .flatMap(
            sqlcmd ->
                client
                    .sendCommand(new QueryPacket(sqlcmd.toString()), true)
                    .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
                    .windowUntil(ServerMessage::resultSetEnd)
                    .map(
                        dataRow ->
                            new MariadbResult(
                                Protocol.TEXT,
                                null,
                                dataRow,
                                ExceptionFactory.INSTANCE,
                                null,
                                false,
                                configuration))
                    .last())
        .then();
  }

  public static Mono<MariadbConnection> closeWithError(Client client, Throwable throwable) {
    return client.close().then(Mono.error(throwable));
  }

  public static Throwable cannotConnect(Throwable throwable, SocketAddress endpoint) {

    if (throwable instanceof R2dbcException) {
      return throwable;
    }

    return new R2dbcNonTransientResourceException(
        String.format("Cannot connect to %s", endpoint), throwable);
  }

  @Override
  public Mono<org.mariadb.r2dbc.api.MariadbConnection> create() {
    ReentrantLock lock = new ReentrantLock();
    return ((configuration.getSocket() != null)
            ? connectToSocket(
                configuration, new DomainSocketAddress(configuration.getSocket()), null, lock)
            : (configuration.getHaMode().equals(HaMode.NONE)
                ? configuration.getHaMode().connectHost(configuration, lock, false)
                : configuration
                    .getHaMode()
                    .connectHost(configuration, lock, false)
                    .flatMap(c -> Mono.just(new FailoverClient(configuration, lock, c)))))
        .flatMap(
            client ->
                Mono.just(
                        new MariadbConnection(
                            client,
                            configuration.getIsolationLevel() == null
                                ? IsolationLevel.REPEATABLE_READ
                                : configuration.getIsolationLevel(),
                            configuration))
                    .onErrorResume(throwable -> closeWithError(client, throwable)))
        .cast(org.mariadb.r2dbc.api.MariadbConnection.class);
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return MariadbConnectionFactoryMetadata.INSTANCE;
  }

  @Override
  public String toString() {
    return "MariadbConnectionFactory{configuration=" + this.configuration + '}';
  }
}

package org.mariadb.r2dbc.client.ha;

import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.r2dbc.MariadbConnection;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.ClientBase;
import org.mariadb.r2dbc.message.flow.AuthenticationFlow;
import org.mariadb.r2dbc.util.HostAddress;
import org.mariadb.r2dbc.util.constants.Capabilities;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;

public class SimpleFactory {

  public static Mono<org.mariadb.r2dbc.api.MariadbConnection> create(
      MariadbConnectionConfiguration configuration) {
    ReentrantLock lock = new ReentrantLock();
    return ((configuration.getSocket() != null)
            ? connectToHost(
                configuration, new DomainSocketAddress(configuration.getSocket()), null, lock)
            : doCreateConnection(configuration, 0, lock))
        .cast(org.mariadb.r2dbc.api.MariadbConnection.class);
  }

  private static Mono<MariadbConnection> doCreateConnection(
      final MariadbConnectionConfiguration configuration, final int idx, ReentrantLock lock) {
    HostAddress hostAddress = configuration.getHostAddresses().get(idx);
    return connectToHost(
            configuration,
            InetSocketAddress.createUnresolved(hostAddress.getHost(), hostAddress.getPort()),
            hostAddress,
            lock)
        .onErrorResume(
            e -> {
              if (idx + 1 < configuration.getHostAddresses().size()) {
                return doCreateConnection(configuration, idx + 1, lock);
              }
              return Mono.error(e);
            });
  }

  public static Mono<MariadbConnection> connectToHost(
      final MariadbConnectionConfiguration configuration,
      SocketAddress endpoint,
      HostAddress hostAddress,
      ReentrantLock lock) {
    return ClientBase.connect(
            ConnectionProvider.newConnection(), endpoint, hostAddress, configuration, lock)
        .delayUntil(client -> AuthenticationFlow.exchange(client, configuration, hostAddress))
        .cast(Client.class)
        .flatMap(
            client ->
                setSessionVariables(configuration, client)
                    .then(
                        Mono.just(
                            new MariadbConnection(
                                client,
                                configuration.getIsolationLevel() == null
                                    ? IsolationLevel.REPEATABLE_READ
                                    : configuration.getIsolationLevel(),
                                configuration)))
                    .onErrorResume(throwable -> closeWithError(client, throwable)))
        .onErrorMap(e -> cannotConnect(e, endpoint));
  }

  private static Mono<Void> setSessionVariables(
      final MariadbConnectionConfiguration configuration, Client client) {

    // set default autocommit value
    StringBuilder sql =
        new StringBuilder("SET autocommit=" + (configuration.autocommit() ? "1" : "0"));

    // set default transaction isolation
    String txIsolation =
        (!client.getVersion().isMariaDBServer()
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
      sql.append(",session_track_system_variables='autocommit,").append(txIsolation).append("'");
    }

    // set session variables if defined
    if (configuration.getSessionVariables() != null
        && configuration.getSessionVariables().size() > 0) {
      Map<String, String> sessionVariable = configuration.getSessionVariables();
      Iterator<String> keys = sessionVariable.keySet().iterator();
      for (int i = 0; i < sessionVariable.size(); i++) {
        String key = keys.next();
        String value = sessionVariable.get(key);
        if (value == null)
          throw new IllegalArgumentException(
              String.format("Session variable '%s' has no value", key));
        sql.append(",").append(key).append("=").append(value);
      }
    }

    return client.executeSimpleCommand(sql.toString()).last().then();
  }

  private static Mono<MariadbConnection> closeWithError(Client client, Throwable throwable) {
    return client.close().then(Mono.error(throwable));
  }

  private static Throwable cannotConnect(Throwable throwable, SocketAddress endpoint) {

    if (throwable instanceof R2dbcException) {
      return throwable;
    }

    return new R2dbcNonTransientResourceException(
        String.format("Cannot connect to %s", endpoint), throwable);
  }
}

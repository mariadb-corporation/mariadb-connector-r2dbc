// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import java.util.ArrayList;
import java.util.List;
import org.mariadb.r2dbc.api.MariadbResult;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ClientPrepareResult;
import reactor.core.publisher.Flux;

/** Basic implementation for batch. //TODO implement bulk */
final class MariadbBatch implements org.mariadb.r2dbc.api.MariadbBatch {

  private final Client client;
  private final MariadbConnectionConfiguration configuration;
  private final List<String> statements = new ArrayList<>();

  MariadbBatch(Client client, MariadbConnectionConfiguration configuration) {
    this.client = client;
    this.configuration = configuration;
  }

  @Override
  public MariadbBatch add(String sql) {
    Assert.requireNonNull(sql, "sql must not be null");

    // ensure commands doesn't have parameters
    if (sql.contains("?") || sql.contains(":")) {
      if (ClientPrepareResult.hasParameter(sql, client.noBackslashEscapes())) {
        throw new IllegalArgumentException(
            String.format("Statement with parameters cannot be batched (sql:'%s')", sql));
      }
    }

    this.statements.add(sql);
    return this;
  }

  @Override
  public Flux<MariadbResult> execute() {
    if (configuration.allowMultiQueries()) {
      return this.client.executeSimpleCommand(String.join(";", this.statements));
    } else {

      Flux<Flux<ServerMessage>> fluxMsg =
          Flux.create(
              sink -> {
                for (String sql : this.statements) {
                  Flux<ServerMessage> in = this.client.sendCommand(new QueryPacket(sql));
                  sink.next(in);
                  in.subscribe();
                }
                sink.complete();
              });

      return fluxMsg
          .flatMap(Flux::from)
          .windowUntil(it -> it.resultSetEnd())
          .map(
              dataRow ->
                  new org.mariadb.r2dbc.client.MariadbResult(
                      true,
                      null,
                      dataRow,
                      ExceptionFactory.INSTANCE,
                      null,
                      client.getVersion().supportReturning(),
                      client.getConf()));
    }
  }
}

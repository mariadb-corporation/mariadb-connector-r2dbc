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

package org.mariadb.r2dbc;

import org.mariadb.r2dbc.api.MariadbResult;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.message.server.ServerMessage;
import org.mariadb.r2dbc.util.Assert;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

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

    if (!MariadbSimpleQueryStatement.supports(sql, this.client)) {
      throw new IllegalArgumentException(
          String.format("Statement with parameters cannot be batched (sql:'%s')", sql));
    }

    this.statements.add(sql);
    return this;
  }

  @Override
  public Flux<MariadbResult> execute() {
    if (configuration.isAllowMultiQueries()) {
      return new MariadbSimpleQueryStatement(this.client, String.join("; ", this.statements))
          .execute();
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
                  new org.mariadb.r2dbc.MariadbResult(
                      true,
                      dataRow,
                      ExceptionFactory.INSTANCE,
                      null,
                      client.getVersion().isMariaDBServer()
                          && client.getVersion().versionGreaterOrEqual(10, 5, 1)));
    }
  }

  @Override
  public String toString() {
    return "MariadbBatch{client=" + this.client + ", statements=" + this.statements + '}';
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mariadb.r2dbc.api.MariadbResult;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.message.Protocol;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ClientParser;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

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
      if (ClientParser.hasParameter(sql, client.noBackslashEscapes())) {
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
      return this.client
          .sendCommand(new QueryPacket(String.join(";", this.statements)), true)
          .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
          .windowUntil(it -> it.resultSetEnd())
          .map(
              dataRow ->
                  new org.mariadb.r2dbc.client.MariadbResult(
                      Protocol.TEXT,
                      null,
                      dataRow,
                      ExceptionFactory.INSTANCE,
                      null,
                      client.getVersion().supportReturning(),
                      configuration));
    } else {
      Iterator<String> iterator = this.statements.iterator();
      Sinks.Many<String> commandsSink = Sinks.many().unicast().onBackpressureBuffer();
      AtomicBoolean canceled = new AtomicBoolean();
      return commandsSink
          .asFlux()
          .map(
              sql ->
                  this.client
                      .sendCommand(new QueryPacket(sql), false)
                      .doOnComplete(() -> tryNextCommand(iterator, commandsSink, canceled))
                      .windowUntil(it -> it.resultSetEnd())
                      .map(
                          dataRow ->
                              new org.mariadb.r2dbc.client.MariadbResult(
                                  Protocol.TEXT,
                                  null,
                                  dataRow,
                                  ExceptionFactory.INSTANCE,
                                  null,
                                  client.getVersion().supportReturning(),
                                  configuration))
                      .cast(org.mariadb.r2dbc.api.MariadbResult.class))
          .flatMap(mariadbResultFlux -> mariadbResultFlux)
          .doOnCancel(() -> canceled.set(true))
          .doOnSubscribe(
              it -> commandsSink.emitNext(iterator.next(), Sinks.EmitFailureHandler.FAIL_FAST));
    }
  }

  private static void tryNextCommand(
      Iterator<String> iterator, Sinks.Many<String> bindingSink, AtomicBoolean canceled) {

    if (canceled.get()) {
      return;
    }

    try {
      if (iterator.hasNext()) {
        bindingSink.emitNext(iterator.next(), Sinks.EmitFailureHandler.FAIL_FAST);
      } else {
        bindingSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
      }
    } catch (Exception e) {
      bindingSink.emitError(e, Sinks.EmitFailureHandler.FAIL_FAST);
    }
  }
}

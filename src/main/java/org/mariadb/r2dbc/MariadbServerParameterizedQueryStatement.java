// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.DecoderState;
import org.mariadb.r2dbc.message.Protocol;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.ExecutePacket;
import org.mariadb.r2dbc.message.client.PreparePacket;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.Binding;
import org.mariadb.r2dbc.util.ServerNamedParamParser;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

final class MariadbServerParameterizedQueryStatement extends MariadbCommonStatement
    implements MariadbStatement {

  private ServerNamedParamParser paramParser;
  private final AtomicReference<ServerPrepareResult> prepareResult;

  MariadbServerParameterizedQueryStatement(
      Client client, String sql, MariadbConnectionConfiguration configuration) {
    super(client, sql, configuration, Protocol.BINARY);
    this.expectedSize = UNKNOWN_SIZE;
    this.paramParser = null;
    this.prepareResult = new AtomicReference<>(client.getPrepareCache().get(sql));
    initializeBinding();
  }

  @Override
  protected int getExpectedSize() {
    if (expectedSize == UNKNOWN_SIZE) {
      expectedSize =
          (prepareResult.get() != null)
              ? prepareResult.get().getNumParams()
              : (((paramParser != null)
                  ? paramParser.getParamCount()
                  : ServerNamedParamParser.parameterParts(
                          initialSql, this.client.noBackslashEscapes())
                      .getParamCount()));
    }
    return expectedSize;
  }

  protected int getColumnIndex(String name) {
    Assert.requireNonNull(name, "identifier cannot be null");
    if (paramParser == null) {
      paramParser =
          ServerNamedParamParser.parameterParts(initialSql, this.client.noBackslashEscapes());
    }
    for (int i = 0; i < this.paramParser.getParamNameList().size(); i++) {
      if (name.equals(this.paramParser.getParamNameList().get(i))) return i;
    }
    throw new NoSuchElementException(
        String.format(
            "No parameter with name '%s' found (possible values %s)",
            name, this.paramParser.getParamNameList().toString()));
  }

  @Override
  public MariadbServerParameterizedQueryStatement returnGeneratedValues(String... columns) {
    Assert.requireNonNull(columns, "columns must not be null");

    if (!client.getVersion().supportReturning() && columns.length > 1) {
      throw new IllegalArgumentException(
          "returnGeneratedValues can have only one column before MariaDB 10.5.1");
    }
    this.generatedColumns = columns;
    return this;
  }

  @Override
  public Flux<org.mariadb.r2dbc.api.MariadbResult> execute() {
    String realSql = paramParser == null ? this.initialSql : paramParser.getRealSql();
    String sql;
    if (this.generatedColumns == null || !client.getVersion().supportReturning()) {
      sql = realSql;
    } else {
      sql = augment(realSql, this.generatedColumns);
    }
    ExceptionFactory factory = ExceptionFactory.withSql(sql);

    if (prepareResult.get() == null && client.getPrepareCache() != null) {
      prepareResult.set(client.getPrepareCache().get(sql));
    }
    if (this.getExpectedSize() != 0) {
      this.getCurrentBinding().validate(this.getExpectedSize());
      return Flux.defer(
          () -> {
            if (this.bindings.size() == 0) {
              // single query
              Binding binding = getCurrentBinding();
              this.initializeBinding();

              if (prepareResult.get() != null) {
                ServerPrepareResult res;
                if (this.client.getPrepareCache() != null
                    && (res = this.client.getPrepareCache().get(sql)) != null
                    && !res.equals(prepareResult.get())) {
                  prepareResult.get().decrementUse(client);
                  prepareResult.set(res);
                }

                if (prepareResult.get().incrementUse()) {
                  Flux<ServerMessage> messages =
                      this.client
                          .sendCommand(
                              new ExecutePacket(
                                  sql,
                                  prepareResult.get(),
                                  binding.getBindResultParameters(
                                      prepareResult.get().getNumParams())),
                              DecoderState.QUERY_RESPONSE,
                              sql,
                              false)
                          .doFinally(s -> prepareResult.get().decrementUse(client));
                  return toResult(Protocol.BINARY, messages, factory, prepareResult);
                } else {
                  // prepare is closing
                  prepareResult.set(null);
                }
              }
              Flux<ServerMessage> messages;
              if (configuration.allowPipelining()
                  && client.getVersion().isMariaDBServer()
                  && client.getVersion().versionGreaterOrEqual(10, 2, 0)) {
                messages =
                    this.client.sendCommand(
                        new PreparePacket(sql),
                        new ExecutePacket(sql, null, binding.getBinds()),
                        false);
              } else {
                messages =
                    client
                        .sendPrepare(new PreparePacket(sql), factory, sql)
                        .flatMapMany(
                            serverPrepareResult -> {
                              prepareResult.set(serverPrepareResult);
                              return this.client.sendCommand(
                                  new ExecutePacket(
                                      sql,
                                      prepareResult.get(),
                                      binding.getBindResultParameters(
                                          prepareResult.get().getNumParams())),
                                  DecoderState.QUERY_RESPONSE,
                                  sql,
                                  false);
                            });
              }
              return toResult(Protocol.BINARY, messages, factory, prepareResult)
                  .doFinally(
                      s -> {
                        if (prepareResult.get() != null) {
                          prepareResult.get().decrementUse(client);
                        }
                      });
            }
            // batch
            bindings.add(getCurrentBinding());
            this.initializeBinding();
            Iterator<Binding> iterator = this.bindings.iterator();
            Sinks.Many<Binding> bindingSink = Sinks.many().unicast().onBackpressureBuffer();
            AtomicBoolean canceled = new AtomicBoolean();
            return prepareIfNotDone(sql, factory)
                .thenMany(
                    bindingSink
                        .asFlux()
                        .doOnComplete(() -> clearBindings(iterator, canceled))
                        .map(
                            binding -> {
                              Flux<ServerMessage> messages =
                                  this.client
                                      .sendCommand(
                                          new ExecutePacket(
                                              sql,
                                              prepareResult.get(),
                                              binding.getBindResultParameters(
                                                  prepareResult.get().getNumParams())),
                                          false)
                                      .doOnComplete(
                                          () -> tryNextBinding(iterator, bindingSink, canceled));

                              return toResult(Protocol.BINARY, messages, factory, prepareResult);
                            })
                        .doOnSubscribe(
                            it ->
                                bindingSink.emitNext(
                                    iterator.next(), Sinks.EmitFailureHandler.FAIL_FAST))
                        .doOnComplete(this.bindings::clear)
                        .doFinally(
                            s -> {
                              if (prepareResult.get() != null) {
                                prepareResult.get().decrementUse(client);
                              }
                            })
                        .doOnCancel(() -> clearBindings(iterator, canceled))
                        .doOnError(e -> clearBindings(iterator, canceled)))
                .flatMap(mariadbResultFlux -> mariadbResultFlux);
          });
    } else {
      return Flux.defer(
          () -> {
            Flux<ServerMessage> messages =
                this.client.sendCommand(
                    new QueryPacket(sql), DecoderState.QUERY_RESPONSE, sql, false);
            return toResult(Protocol.TEXT, messages, factory, null);
          });
    }
  }

  private Mono<ServerPrepareResult> prepareIfNotDone(String sql, ExceptionFactory factory) {
    // prepare command, if not already done
    if (prepareResult.get() == null) {
      prepareResult.set(client.getPrepareCache().get(sql));
      if (prepareResult.get() == null) {
        return client
            .sendPrepare(new PreparePacket(sql), factory, sql)
            .doOnSuccess(p -> prepareResult.set(p));
      }
    }
    prepareResult.get().incrementUse();
    return Mono.just(prepareResult.get());
  }

  @Override
  public String toString() {
    List<Binding> tmpBindings = new ArrayList<>();
    tmpBindings.addAll(bindings);
    tmpBindings.add(getCurrentBinding());
    return "MariadbServerParameterizedQueryStatement{"
        + "client="
        + client
        + ", sql='"
        + initialSql
        + '\''
        + ", bindings="
        + Arrays.toString(tmpBindings.toArray())
        + ", configuration="
        + configuration
        + ", generatedColumns="
        + (generatedColumns != null ? Arrays.toString(generatedColumns) : null)
        + ", prepareResult="
        + prepareResult.get()
        + '}';
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.DecoderState;
import org.mariadb.r2dbc.message.Protocol;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.message.client.QueryWithParametersPacket;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.Binding;
import org.mariadb.r2dbc.util.ClientParser;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

final class MariadbClientParameterizedQueryStatement extends MariadbCommonStatement {

  private ClientParser parser;

  MariadbClientParameterizedQueryStatement(
      Client client, String sql, MariadbConnectionConfiguration configuration) {
    super(client, sql, configuration, Protocol.TEXT);
    this.parser = ClientParser.parameterParts(this.initialSql, this.client.noBackslashEscapes());
    this.expectedSize = this.parser.getParamCount();
  }

  protected int getColumnIndex(String name) {
    Assert.requireNonNull(name, "identifier cannot be null");
    for (int i = 0; i < this.parser.getParamNameList().size(); i++) {
      if (name.equals(this.parser.getParamNameList().get(i))) return i;
    }
    if (parser.getParamCount() <= 0) {
      throw new IndexOutOfBoundsException(
          String.format("Binding parameters is not supported for the statement '%s'", initialSql));
    }
    throw new NoSuchElementException(
        String.format(
            "No parameter with name '%s' found (possible values %s)",
            name, this.parser.getParamNameList()));
  }

  @Override
  public Flux<org.mariadb.r2dbc.api.MariadbResult> execute() {
    String sql;
    ExceptionFactory factory;
    if (this.generatedColumns == null || !client.getVersion().supportReturning()) {
      sql = this.initialSql;
      factory = this.factory;
    } else {
      sql = augment(this.initialSql, this.generatedColumns);
      factory = ExceptionFactory.withSql(sql);
    }

    if (this.getExpectedSize() != 0) {
      if (this.bindings.size() == 0) {
        throw new IllegalStateException("No parameters have been set");
      }
      this.bindings.forEach(b -> b.validate(this.getExpectedSize()));
      return Flux.defer(
          () -> {
            if (this.bindings.size() == 1) {
              // single query
              Binding binding = this.bindings.pollFirst();

              Flux<ServerMessage> messages =
                  bindingParameterResults(binding, getExpectedSize())
                      .flatMapMany(
                          values ->
                              this.client
                                  .sendCommand(
                                      new QueryWithParametersPacket(
                                          parser,
                                          values,
                                          client.getVersion().supportReturning()
                                              ? generatedColumns
                                              : null),
                                      false)
                                  .doFinally(s -> values.stream().forEach(bind -> bind.release())));
              return toResult(
                  Protocol.TEXT, client, messages, factory, null, generatedColumns, configuration);
            }

            // batch
            Iterator<Binding> iterator = this.bindings.iterator();
            Sinks.Many<Binding> bindingSink = Sinks.many().unicast().onBackpressureBuffer();
            AtomicBoolean canceled = new AtomicBoolean();
            return bindingSink
                .asFlux()
                .map(
                    it -> {
                      Flux<ServerMessage> messages =
                          bindingParameterResults(it, getExpectedSize())
                              .flatMapMany(
                                  values ->
                                      this.client
                                          .sendCommand(
                                              new QueryWithParametersPacket(
                                                  parser,
                                                  values,
                                                  client.getVersion().supportReturning()
                                                      ? generatedColumns
                                                      : null),
                                              false)
                                          .doFinally(
                                              s -> values.stream().forEach(bind -> bind.release())))
                              .doOnComplete(() -> tryNextBinding(iterator, bindingSink, canceled));

                      return toResult(
                          Protocol.TEXT,
                          this.client,
                          messages,
                          factory,
                          null,
                          generatedColumns,
                          configuration);
                    })
                .flatMap(mariadbResultFlux -> mariadbResultFlux)
                .doOnCancel(() -> clearBindings(iterator, canceled))
                .doOnError(e -> clearBindings(iterator, canceled))
                .doOnSubscribe(
                    it ->
                        bindingSink.emitNext(iterator.next(), Sinks.EmitFailureHandler.FAIL_FAST));
          });
    } else {
      return Flux.defer(
          () -> {
            Flux<ServerMessage> messages =
                this.client.sendCommand(
                    new QueryPacket(sql), DecoderState.QUERY_RESPONSE, sql, false);
            return toResult(
                Protocol.TEXT, client, messages, factory, null, generatedColumns, configuration);
          });
    }
  }

  @Override
  public MariadbClientParameterizedQueryStatement returnGeneratedValues(String... columns) {
    Assert.requireNonNull(columns, "columns must not be null");
    if (!client.getVersion().supportReturning() && columns.length > 1) {
      throw new IllegalArgumentException(
          "returnGeneratedValues can have only one column before MariaDB 10.5.1");
    }
    if (parser.supportAddingReturning() == null)
      parser = ClientParser.parameterParts(this.initialSql, this.client.noBackslashEscapes());
    parser.validateAddingReturning();
    this.generatedColumns = columns;
    return this;
  }

  @Override
  public String toString() {
    return "MariadbClientParameterizedQueryStatement{"
        + "client="
        + client
        + ", sql='"
        + initialSql
        + '\''
        + ", bindings="
        + Arrays.toString(bindings.toArray())
        + ", configuration="
        + configuration
        + ", generatedColumns="
        + Arrays.toString(generatedColumns)
        + '}';
  }
}

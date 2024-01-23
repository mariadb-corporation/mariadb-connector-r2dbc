// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

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
    initializeBinding();
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
      this.getCurrentBinding().validate(this.getExpectedSize());
      return Flux.defer(
          () -> {
            if (this.bindings.size() == 0) {
              // single query
              Binding binding = this.getCurrentBinding();
              this.initializeBinding();

              Flux<ServerMessage> messages =
                  this.client.sendCommand(
                      new QueryWithParametersPacket(
                          parser,
                          binding.getBindResultParameters(getExpectedSize()),
                          client.getVersion().supportReturning() ? generatedColumns : null),
                      false);
              return toResult(Protocol.TEXT, messages, factory, null);
            }

            // batch
            bindings.add(getCurrentBinding());
            this.initializeBinding();
            Iterator<Binding> iterator = this.bindings.iterator();
            Sinks.Many<Binding> bindingSink = Sinks.many().unicast().onBackpressureBuffer();
            AtomicBoolean canceled = new AtomicBoolean();
            return bindingSink
                .asFlux()
                .doOnComplete(() -> clearBindings(iterator, canceled))
                .map(
                    it -> {
                      Flux<ServerMessage> messages =
                          this.client
                              .sendCommand(
                                  new QueryWithParametersPacket(
                                      parser,
                                      it.getBindResultParameters(getExpectedSize()),
                                      client.getVersion().supportReturning()
                                          ? generatedColumns
                                          : null),
                                  false)
                              .doOnComplete(() -> tryNextBinding(iterator, bindingSink, canceled));

                      return toResult(Protocol.TEXT, messages, factory, null);
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
            return toResult(Protocol.TEXT, messages, factory, null);
          });
    }
  }

  @Override
  public MariadbClientParameterizedQueryStatement returnGeneratedValues(String... columns) {
    Assert.requireNonNull(columns, "columns must not be null");
    if (parser.supportAddingReturning() == null)
      parser =
          ClientParser.parameterPartsCheckReturning(
              this.initialSql, this.client.noBackslashEscapes());
    if (!client.getVersion().supportReturning() && columns.length > 1) {
      throw new IllegalArgumentException(
          "returnGeneratedValues can have only one column before MariaDB 10.5.1");
    }
    parser.validateAddingReturning();
    this.generatedColumns = columns;
    return this;
  }

  @Override
  public String toString() {
    List<Binding> tmpBindings = new ArrayList<>();
    tmpBindings.addAll(bindings);
    tmpBindings.add(getCurrentBinding());

    return "MariadbClientParameterizedQueryStatement{"
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
        + Arrays.toString(generatedColumns)
        + '}';
  }
}

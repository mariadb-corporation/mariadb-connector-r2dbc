// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.MariadbResult;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.codec.ParameterWithCodec;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.QueryWithParametersPacket;
import org.mariadb.r2dbc.message.server.RowPacket;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ClientPrepareResult;
import org.mariadb.r2dbc.util.MariadbType;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

final class MariadbClientParameterizedQueryStatement implements MariadbStatement {

  private final Client client;
  private final String sql;
  private final ClientPrepareResult prepareResult;
  private final MariadbConnectionConfiguration configuration;
  private ParameterWithCodec[] parameters;
  private List<ParameterWithCodec[]> batchingParameters;
  private String[] generatedColumns;

  MariadbClientParameterizedQueryStatement(
      Client client, String sql, MariadbConnectionConfiguration configuration) {
    this.client = client;
    this.configuration = configuration;
    this.sql = Assert.requireNonNull(sql, "sql must not be null");
    this.prepareResult =
        ClientPrepareResult.parameterParts(this.sql, this.client.noBackslashEscapes());
    this.parameters = null;
  }

  @Override
  public MariadbClientParameterizedQueryStatement add() {
    Assert.requireNonNull(this.parameters, "add() cannot be used if not bindings where set");

    // check valid parameters
    for (int i = 0; i < prepareResult.getParamCount(); i++) {
      if (parameters[i] == null) {
        throw new IllegalStateException(String.format("Parameter at position %s is not set", i));
      }
    }
    if (batchingParameters == null) batchingParameters = new ArrayList<>();
    batchingParameters.add(parameters);
    parameters = null;
    return this;
  }

  @Override
  public MariadbClientParameterizedQueryStatement bind(
      @Nullable String identifier, @Nullable Object value) {
    Assert.requireNonNull(identifier, "identifier cannot be null");
    Assert.requireNonNull(value, "value cannot be null or use bindNull");
    return bind(getColumn(identifier), value);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public MariadbClientParameterizedQueryStatement bind(int index, @Nullable Object value) {
    if (prepareResult.getParamCount() <= 0) {
      throw new IndexOutOfBoundsException(
          String.format("Binding parameters is not supported for the statement '%s'", sql));
    }
    if (index >= prepareResult.getParamCount() || index < 0) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is %d",
              prepareResult.getParamCount() - 1, index));
    }
    Assert.requireNonNull(value, "value cannot be null or use bindNull");
    if (parameters == null) parameters = new ParameterWithCodec[prepareResult.getParamCount()];

    parameters[index] =
        new ParameterWithCodec(
            (value instanceof Parameter) ? (Parameter) value : Parameters.in(value),
            Codecs.codecByClass(value.getClass(), index));
    return this;
  }

  @Override
  public MariadbClientParameterizedQueryStatement bindNull(
      @Nullable String identifier, @Nullable Class<?> type) {
    Assert.requireNonNull(identifier, "identifier cannot be null");
    return bindNull(getColumn(identifier), type);
  }

  @Override
  public MariadbClientParameterizedQueryStatement bindNull(int index, @Nullable Class<?> type) {
    if (prepareResult.getParamCount() <= 0) {
      throw new IndexOutOfBoundsException(
          String.format("Binding parameters is not supported for the statement '%s'", sql));
    }
    if (index >= prepareResult.getParamCount() || index < 0) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is " + "%d",
              prepareResult.getParamCount() - 1, index));
    }
    if (parameters == null) parameters = new ParameterWithCodec[prepareResult.getParamCount()];
    parameters[index] =
        new ParameterWithCodec(
            (type == null) ? Parameters.in(MariadbType.VARCHAR) : Parameters.in(type),
            Codecs.codecByClass((type == null ? String.class : type), index));
    return this;
  }

  private int getColumn(String name) {
    for (int i = 0; i < this.prepareResult.getParamNameList().size(); i++) {
      if (name.equals(this.prepareResult.getParamNameList().get(i))) return i;
    }
    if (prepareResult.getParamCount() <= 0) {
      throw new IndexOutOfBoundsException(
          String.format("Binding parameters is not supported for the statement '%s'", sql));
    }
    throw new NoSuchElementException(
        String.format(
            "No parameter with name '%s' found (possible values %s)",
            name, this.prepareResult.getParamNameList().toString()));
  }

  @Override
  public Flux<org.mariadb.r2dbc.api.MariadbResult> execute() {

    if (batchingParameters == null) {
      if (parameters == null) {
        if (prepareResult.getParamCount() > 0) {
          throw new IllegalStateException("No parameter have been set");
        }
      } else {
        // valid parameters
        for (int i = 0; i < prepareResult.getParamCount(); i++) {
          if (parameters[i] == null) {
            throw new IllegalStateException(
                String.format("Parameter at position %s is not set", i));
          }
        }
      }
      return executeSingleQuery(this.sql, this.prepareResult, this.generatedColumns);
    } else {
      // add current set of parameters. see https://github.com/r2dbc/r2dbc-spi/issues/229
      add();
      ExceptionFactory factory = ExceptionFactory.withSql(sql);
      String[] generatedCols =
          generatedColumns != null && client.getVersion().supportReturning()
              ? generatedColumns
              : null;
      Flux<ServerMessage> fluxMsg =
          this.client.sendCommand(
              new QueryWithParametersPacket(
                  prepareResult, this.batchingParameters.get(0), generatedCols, factory));
      int index = 1;
      while (index < this.batchingParameters.size()) {
        fluxMsg =
            fluxMsg.concatWith(
                this.client.sendCommand(
                    new QueryWithParametersPacket(
                        prepareResult,
                        this.batchingParameters.get(index++),
                        generatedCols,
                        factory)));
      }
      this.batchingParameters = null;
      this.parameters = null;

      Flux<org.mariadb.r2dbc.api.MariadbResult> flux =
          fluxMsg
              .windowUntil(it -> it.resultSetEnd())
              .map(
                  dataRow ->
                      new MariadbResult(
                          true,
                          null,
                          dataRow,
                          ExceptionFactory.INSTANCE,
                          generatedColumns,
                          client.getVersion().supportReturning(),
                          client.getConf()));
      return flux.doOnDiscard(RowPacket.class, RowPacket::release);
    }
  }

  @Override
  public MariadbClientParameterizedQueryStatement returnGeneratedValues(String... columns) {
    Assert.requireNonNull(columns, "columns must not be null");

    if (!client.getVersion().supportReturning() && columns.length > 1) {
      throw new IllegalArgumentException(
          "returnGeneratedValues can have only one column before MariaDB 10.5.1");
    }
    prepareResult.validateAddingReturning();
    this.generatedColumns = columns;
    return this;
  }

  private Flux<org.mariadb.r2dbc.api.MariadbResult> executeSingleQuery(
      String sql, ClientPrepareResult prepareResult, String[] generatedColumns) {
    ExceptionFactory factory = ExceptionFactory.withSql(sql);

    Flux<org.mariadb.r2dbc.api.MariadbResult> response =
        this.client
            .sendCommand(
                new QueryWithParametersPacket(
                    prepareResult,
                    parameters,
                    generatedColumns != null && client.getVersion().supportReturning()
                        ? generatedColumns
                        : null,
                    factory))
            .windowUntil(it -> it.resultSetEnd())
            .map(
                dataRow ->
                    new MariadbResult(
                        true,
                        null,
                        dataRow,
                        factory,
                        generatedColumns,
                        client.getVersion().supportReturning(),
                        client.getConf()));
    return response
        .concatWith(
            Flux.create(
                sink -> {
                  sink.complete();
                  parameters = null;
                }))
        .doOnDiscard(RowPacket.class, RowPacket::release);
  }

  @Override
  public String toString() {
    return "MariadbClientParameterizedQueryStatement{"
        + "client="
        + client
        + ", sql='"
        + sql
        + '\''
        + ", prepareResult="
        + prepareResult
        + ", parameters="
        + Arrays.toString(parameters)
        + ", configuration="
        + configuration
        + ", generatedColumns="
        + Arrays.toString(generatedColumns)
        + '}';
  }
}

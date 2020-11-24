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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.codec.Parameter;
import org.mariadb.r2dbc.message.client.QueryWithParametersPacket;
import org.mariadb.r2dbc.message.server.ServerMessage;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ClientPrepareResult;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

final class MariadbClientParameterizedQueryStatement implements MariadbStatement {

  private final Client client;
  private final String sql;
  private final ClientPrepareResult prepareResult;
  private final MariadbConnectionConfiguration configuration;
  private Parameter<?>[] parameters;
  private List<Parameter<?>[]> batchingParameters;
  private String[] generatedColumns;

  MariadbClientParameterizedQueryStatement(
      Client client, String sql, MariadbConnectionConfiguration configuration) {
    this.client = client;
    this.configuration = configuration;
    this.sql = Assert.requireNonNull(sql, "sql must not be null");
    this.prepareResult =
        ClientPrepareResult.parameterParts(this.sql, this.client.noBackslashEscapes());
    this.parameters = new Parameter<?>[prepareResult.getParamCount()];
  }

  @Override
  public MariadbClientParameterizedQueryStatement add() {
    // check valid parameters
    for (int i = 0; i < prepareResult.getParamCount(); i++) {
      if (parameters[i] == null) {
        throw new IllegalArgumentException(String.format("Parameter at position %s is not set", i));
      }
    }
    if (batchingParameters == null) batchingParameters = new ArrayList<>();
    batchingParameters.add(parameters);
    parameters = new Parameter<?>[prepareResult.getParamCount()];
    return this;
  }

  @Override
  public MariadbClientParameterizedQueryStatement bind(
      @Nullable String identifier, @Nullable Object value) {
    Assert.requireNonNull(identifier, "identifier cannot be null");
    return bind(getColumn(identifier), value);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public MariadbClientParameterizedQueryStatement bind(int index, @Nullable Object value) {
    if (value == null) return bindNull(index, null);
    if (index >= prepareResult.getParamCount() || index < 0) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is %d",
              prepareResult.getParamCount() - 1, index));
    }

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canEncode(value.getClass())) {
        parameters[index] = (Parameter<?>) new Parameter(codec, value);
        return this;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "No encoder for class %s (parameter at index %s) ", value.getClass().getName(), index));
  }

  @Override
  public MariadbClientParameterizedQueryStatement bindNull(
      @Nullable String identifier, @Nullable Class<?> type) {
    Assert.requireNonNull(identifier, "identifier cannot be null");
    return bindNull(getColumn(identifier), type);
  }

  @Override
  public MariadbClientParameterizedQueryStatement bindNull(int index, @Nullable Class<?> type) {
    if (index >= prepareResult.getParamCount() || index < 0) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is " + "%d",
              prepareResult.getParamCount() - 1, index));
    }
    parameters[index] = Parameter.NULL_PARAMETER;
    return this;
  }

  private int getColumn(String name) {
    for (int i = 0; i < this.prepareResult.getParamNameList().size(); i++) {
      if (name.equals(this.prepareResult.getParamNameList().get(i))) return i;
    }
    throw new IllegalArgumentException(
        String.format(
            "No parameter with name '%s' found (possible values %s)",
            name, this.prepareResult.getParamNameList().toString()));
  }

  @Override
  public Flux<org.mariadb.r2dbc.api.MariadbResult> execute() {

    if (batchingParameters == null) {
      // valid parameters
      for (int i = 0; i < prepareResult.getParamCount(); i++) {
        if (parameters[i] == null) {
          throw new IllegalArgumentException(
              String.format("Parameter at position %s is not set", i));
        }
      }
      return execute(this.sql, this.prepareResult, this.generatedColumns);
    } else {
      Flux<ServerMessage> fluxMsg =
          this.client.sendCommand(
              new QueryWithParametersPacket(
                  prepareResult,
                  this.batchingParameters.get(0),
                  generatedColumns != null && client.getVersion().supportReturning()
                      ? generatedColumns
                      : null));
      int index = 1;
      while (index < this.batchingParameters.size()) {
        fluxMsg =
            fluxMsg.concatWith(
                this.client.sendCommand(
                    new QueryWithParametersPacket(
                        prepareResult,
                        this.batchingParameters.get(index++),
                        generatedColumns != null && client.getVersion().supportReturning()
                            ? generatedColumns
                            : null)));
      }

      this.batchingParameters.clear();
      this.parameters = new Parameter<?>[prepareResult.getParamCount()];

      return fluxMsg
          .windowUntil(it -> it.resultSetEnd())
          .map(
              dataRow ->
                  new org.mariadb.r2dbc.MariadbResult(
                      true,
                      dataRow,
                      ExceptionFactory.INSTANCE,
                      generatedColumns,
                      client.getVersion().supportReturning()));
    }
  }

  @Override
  public MariadbClientParameterizedQueryStatement fetchSize(int rows) {
    return this;
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

  private Flux<org.mariadb.r2dbc.api.MariadbResult> execute(
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
                        : null))
            .windowUntil(it -> it.resultSetEnd())
            .map(
                dataRow ->
                    new MariadbResult(
                        true,
                        dataRow,
                        factory,
                        generatedColumns,
                        client.getVersion().supportReturning()));
    return response.concatWith(
        Flux.create(
            sink -> {
              sink.complete();
              parameters = new Parameter<?>[prepareResult.getParamCount()];
            }));
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

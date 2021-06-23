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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.DecoderState;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.codec.Parameter;
import org.mariadb.r2dbc.message.client.ExecutePacket;
import org.mariadb.r2dbc.message.client.PreparePacket;
import org.mariadb.r2dbc.message.server.CompletePrepareResult;
import org.mariadb.r2dbc.message.server.ServerMessage;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

final class MariadbServerParameterizedQueryStatement implements MariadbStatement {

  private final Client client;
  private final String initialSql;
  private final MariadbConnectionConfiguration configuration;
  private Map<Integer, Parameter<?>> parameters;
  private List<Map<Integer, Parameter<?>>> batchingParameters;
  private String[] generatedColumns;
  private AtomicReference<ServerPrepareResult> prepareResult;

  MariadbServerParameterizedQueryStatement(
      Client client, String sql, MariadbConnectionConfiguration configuration) {
    this.client = client;
    this.configuration = configuration;
    this.initialSql = Assert.requireNonNull(sql, "sql must not be null");
    this.parameters = null;
    this.prepareResult = new AtomicReference<>(client.getPrepareCache().get(sql));
  }

  @Override
  public MariadbServerParameterizedQueryStatement add() {
    // check valid parameters
    if (this.parameters != null) {
      if (prepareResult.get() != null) {
        for (int i = 0; i < prepareResult.get().getNumParams(); i++) {
          if (parameters.get(i) == null) {
            throw new IllegalArgumentException(
                String.format("Parameter at position %s is not set", i));
          }
        }
      }
      if (batchingParameters == null) batchingParameters = new ArrayList<>();
      batchingParameters.add(parameters);
      parameters = null;
    }
    return this;
  }

  @Override
  public MariadbServerParameterizedQueryStatement bind(
      @Nullable String identifier, @Nullable Object value) {
    Assert.requireNonNull(identifier, "identifier cannot be null");
    return bind(getColumn(identifier), value);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public MariadbServerParameterizedQueryStatement bind(int index, @Nullable Object value) {
    if (index < 0) {
      throw new IndexOutOfBoundsException(
          String.format("wrong index value %d, index must be positive", index));
    }

    if (prepareResult.get() != null && index >= prepareResult.get().getNumParams()) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is %d",
              prepareResult.get().getNumParams() - 1, index));
    }
    if (value == null) return bindNull(index, null);
    if (parameters == null) parameters = new HashMap<>();
    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canEncode(value.getClass())) {
        parameters.put(index, (Parameter<?>) new Parameter(codec, value));
        return this;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "No encoder for class %s (parameter at index %s) ", value.getClass().getName(), index));
  }

  @Override
  public MariadbServerParameterizedQueryStatement bindNull(
      @Nullable String identifier, @Nullable Class<?> type) {
    Assert.requireNonNull(identifier, "identifier cannot be null");
    return bindNull(getColumn(identifier), type);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public MariadbServerParameterizedQueryStatement bindNull(int index, @Nullable Class<?> type) {
    if (index < 0) {
      throw new IndexOutOfBoundsException(
          String.format("wrong index value %d, index must be positive", index));
    }

    if (prepareResult.get() != null && index >= prepareResult.get().getNumParams()) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is %d",
              prepareResult.get().getNumParams() - 1, index));
    }
    Parameter<?> parameter = null;
    if (type != null) {
      for (Codec<?> codec : Codecs.LIST) {
        if (codec.canEncode(type)) {

          parameter =
              new Parameter(codec, null) {
                @Override
                public DataType getBinaryEncodeType() {
                  return DataType.VARCHAR;
                }

                @Override
                public boolean isNull() {
                  return true;
                }
              };
          break;
        }
      }
    }
    if (parameter == null) {
      parameter = Parameter.NULL_PARAMETER;
    }
    if (parameters == null) parameters = new HashMap<>();
    parameters.put(index, parameter);
    return this;
  }

  private int getColumn(String name) {
    throw new IllegalArgumentException("Cannot use getColumn(name) with prepared statement");
  }

  private void validateParameters() {
    if (prepareResult.get() != null && this.parameters != null) {
      // valid parameters
      for (int i = 0; i < prepareResult.get().getNumParams(); i++) {
        if (parameters.get(i) == null) {
          throw new IllegalArgumentException(
              String.format("Parameter at position %s is not set", i));
        }
      }
    }
  }

  @Override
  public Flux<org.mariadb.r2dbc.api.MariadbResult> execute() {
    String sql = this.initialSql;
    if (client.getVersion().supportReturning() && generatedColumns != null) {
      sql +=
          generatedColumns.length == 0
              ? " RETURNING *"
              : " RETURNING " + String.join(", ", generatedColumns);
      prepareResult.set(client.getPrepareCache().get(sql));
    }

    if (batchingParameters == null) {
      validateParameters();
      return executeSingleQuery(sql, parameters, this.generatedColumns);
    } else {
      // add current set of parameters. see https://github.com/r2dbc/r2dbc-spi/issues/229
      this.add();

      // prepare command, if not already done
      if (prepareResult.get() == null) {
        prepareResult.set(client.getPrepareCache().get(sql));
        if (prepareResult.get() == null) {
          sendPrepare(sql).block();
        }
      }

      Flux<ServerMessage> fluxMsg =
          this.client.sendCommand(
              new ExecutePacket(
                  prepareResult.get().getStatementId(), this.batchingParameters.get(0)));
      int index = 1;
      while (index < this.batchingParameters.size()) {
        fluxMsg =
            fluxMsg.concatWith(
                this.client.sendCommand(
                    new ExecutePacket(
                        prepareResult.get().getStatementId(),
                        this.batchingParameters.get(index++))));
      }
      fluxMsg =
          fluxMsg.concatWith(
              Flux.create(
                  sink -> {
                    prepareResult.get().decrementUse(client);
                    sink.complete();
                  }));

      this.batchingParameters = null;
      this.parameters = null;

      return fluxMsg
          .windowUntil(it -> it.resultSetEnd())
          .map(
              dataRow ->
                  new MariadbResult(
                      false,
                      prepareResult,
                      dataRow,
                      ExceptionFactory.INSTANCE,
                      null,
                      client.getVersion().supportReturning(),
                      client.getConf()));
    }
  }

  @Override
  public MariadbServerParameterizedQueryStatement fetchSize(int rows) {
    return this;
  }

  @Override
  public MariadbServerParameterizedQueryStatement returnGeneratedValues(String... columns) {
    Assert.requireNonNull(columns, "columns must not be null");

    if (!client.getVersion().supportReturning() && columns.length > 1) {
      throw new IllegalArgumentException(
          "returnGeneratedValues can have only one column before MariaDB 10.5.1");
    }
    //    prepareResult.validateAddingReturning();
    this.generatedColumns = columns;
    return this;
  }

  private Flux<org.mariadb.r2dbc.api.MariadbResult> executeSingleQuery(
      String sql, Map<Integer, Parameter<?>> parameters, String[] generatedColumns) {
    ExceptionFactory factory = ExceptionFactory.withSql(sql);

    if (prepareResult.get() == null && client.getPrepareCache() != null) {
      prepareResult.set(client.getPrepareCache().get(sql));
    }

    if (prepareResult.get() != null) {
      validateParameters();

      ServerPrepareResult res;
      if (this.client.getPrepareCache() != null
          && (res = this.client.getPrepareCache().get(sql)) != null
          && !res.equals(prepareResult.get())) {
        prepareResult.get().decrementUse(client);
        prepareResult.set(res);
      }

      if (prepareResult.get().incrementUse()) {
        return sendExecuteCmd(factory, parameters, generatedColumns)
            .concatWith(
                Flux.create(
                    sink -> {
                      prepareResult.get().decrementUse(client);
                      sink.complete();
                      parameters.clear();
                    }));
      } else {
        // prepare is closing
        prepareResult.set(null);
      }
    }

    Flux<org.mariadb.r2dbc.api.MariadbResult> flux;
    if (configuration.allowPipelining()
        && client.getVersion().isMariaDBServer()
        && client.getVersion().versionGreaterOrEqual(10, 2, 0)) {
      flux = sendPrepareAndExecute(sql, factory, parameters, generatedColumns);
    } else {
      flux =
          sendPrepare(sql)
              .flatMapMany(
                  prepareResult1 -> {
                    prepareResult.set(prepareResult1);
                    return sendExecuteCmd(factory, parameters, generatedColumns);
                  });
    }
    return flux.concatWith(
        Flux.create(
            sink -> {
              prepareResult.set(client.getPrepareCache().get(sql));
              if (prepareResult.get() != null) {
                prepareResult.get().decrementUse(client);
              }
              sink.complete();
              parameters.clear();
            }));
  }

  private Flux<org.mariadb.r2dbc.api.MariadbResult> sendPrepareAndExecute(
      String sql,
      ExceptionFactory factory,
      Map<Integer, Parameter<?>> parameters,
      String[] generatedColumns) {
    return this.client
        .sendCommand(new PreparePacket(sql), new ExecutePacket(-1, parameters))
        .windowUntil(it -> it.resultSetEnd())
        .map(
            dataRow ->
                new MariadbResult(
                    false,
                    this.prepareResult,
                    dataRow,
                    factory,
                    generatedColumns,
                    client.getVersion().supportReturning(),
                    client.getConf()));
  }

  private Mono<ServerPrepareResult> sendPrepare(String sql) {
    Flux<ServerPrepareResult> f =
        this.client
            .sendCommand(new PreparePacket(sql), DecoderState.PREPARE_RESPONSE, sql)
            .handle(
                (it, sink) -> {
                  if (it instanceof CompletePrepareResult) {
                    prepareResult.set(((CompletePrepareResult) it).getPrepare());
                    sink.next(prepareResult.get());
                  }
                  if (it.ending()) sink.complete();
                });
    return f.singleOrEmpty();
  }

  private Flux<org.mariadb.r2dbc.api.MariadbResult> sendExecuteCmd(
      ExceptionFactory factory, Map<Integer, Parameter<?>> parameters, String[] generatedColumns) {
    return this.client
        .sendCommand(
            new ExecutePacket(
                prepareResult.get() != null ? prepareResult.get().getStatementId() : -1,
                parameters))
        .windowUntil(it -> it.resultSetEnd())
        .map(
            dataRow ->
                new MariadbResult(
                    false,
                    prepareResult,
                    dataRow,
                    factory,
                    generatedColumns,
                    client.getVersion().supportReturning(),
                    client.getConf()));
  }

  @Override
  public String toString() {
    return "MariadbServerParameterizedQueryStatement{"
        + "client="
        + client
        + ", sql='"
        + initialSql
        + '\''
        + ", configuration="
        + configuration
        + ", parameters="
        + parameters
        + ", batchingParameters="
        + batchingParameters
        + ", generatedColumns="
        + (generatedColumns != null ? Arrays.toString(generatedColumns) : null)
        + ", prepareResult="
        + prepareResult.get()
        + '}';
  }
}

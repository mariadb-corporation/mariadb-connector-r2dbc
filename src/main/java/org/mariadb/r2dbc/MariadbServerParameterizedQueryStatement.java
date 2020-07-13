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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.client.DecoderState;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.codec.Parameter;
import org.mariadb.r2dbc.message.client.ExecutePacket;
import org.mariadb.r2dbc.message.client.PreparePacket;
import org.mariadb.r2dbc.message.server.PrepareResultPacket;
import org.mariadb.r2dbc.message.server.ServerMessage;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.BufferUtils;
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
  private ServerPrepareResult prepareResult;

  MariadbServerParameterizedQueryStatement(
      Client client, String sql, MariadbConnectionConfiguration configuration) {
    this.client = client;
    this.configuration = configuration;
    this.initialSql = Assert.requireNonNull(sql, "sql must not be null");
    this.parameters = new HashMap<>();
    this.prepareResult = client.getPrepareCache().get(sql);
  }

  static boolean supports(String sql) {
    Assert.requireNonNull(sql, "sql must not be null");
    return !sql.trim().isEmpty();
  }

  @Override
  public MariadbServerParameterizedQueryStatement add() {
    // check valid parameters
    if (prepareResult != null) {
      for (int i = 0; i < prepareResult.getNumParams(); i++) {
        if (parameters.get(i) == null) {
          throw new IllegalArgumentException(
              String.format("Parameter at position %i is not set", i));
        }
      }
    }
    if (batchingParameters == null) batchingParameters = new ArrayList<>();
    batchingParameters.add(parameters);
    parameters = new HashMap<>();
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

    if (prepareResult != null && index >= prepareResult.getNumParams()) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is %d",
              prepareResult.getNumParams() - 1, index));
    }
    if (value == null) return bindNull(index, null);

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canEncode(value)) {

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

    if (prepareResult != null && index >= prepareResult.getNumParams()) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is %d",
              prepareResult.getNumParams() - 1, index));
    }
    Parameter<?> parameter = null;
    if (type != null) {
      for (Codec<?> codec : Codecs.LIST) {
        if (codec.canEncode(type)) {

          parameter =
              new Parameter(codec, null) {
                @Override
                public void encodeText(ByteBuf out, Context context) {
                  BufferUtils.writeAscii(out, "null");
                }

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
    parameters.put(index, parameter);
    return this;
  }

  private int getColumn(String name) {
    // TODO handle prepare response first ?
    throw new IllegalArgumentException("Cannot use getColumn(name) with prepared statement");
  }

  private void validateParameters() {
    if (prepareResult != null) {
      // valid parameters
      for (int i = 0; i < prepareResult.getNumParams(); i++) {
        if (parameters.get(i) == null) {
          prepareResult.close(client);
          throw new IllegalArgumentException(
              String.format("Parameter at position %s is not set", i));
        }
      }
    }
  }

  @Override
  public Flux<org.mariadb.r2dbc.api.MariadbResult> execute() {
    validateParameters();
    String sql = this.initialSql;
    if (generatedColumns != null) {
      sql +=
          generatedColumns.length == 0
              ? " RETURNING *"
              : " RETURNING " + String.join(", ", generatedColumns);
      prepareResult = null;
      prepareResult = client.getPrepareCache().get(sql);
    }

    if (batchingParameters == null) {
      return execute(sql, parameters, this.generatedColumns);
    } else {
      add();
      if (prepareResult == null) {
        prepareResult = client.getPrepareCache().get(sql);
        if (prepareResult == null) {
          sendPrepare(sql).block();
        }
      }
      Flux<ServerMessage> fluxMsg =
          this.client.sendCommand(
              new ExecutePacket(prepareResult.getStatementId(), this.batchingParameters.get(0)));
      int index = 1;
      while (index < this.batchingParameters.size()) {
        fluxMsg =
            fluxMsg.concatWith(
                this.client.sendCommand(
                    new ExecutePacket(
                        prepareResult.getStatementId(), this.batchingParameters.get(index++))));
      }
      fluxMsg =
          fluxMsg.concatWith(
              Flux.create(
                  sink -> {
                    prepareResult.decrementUse(client);
                    sink.complete();
                  }));

      this.batchingParameters.clear();
      this.parameters = new HashMap<>();

      return fluxMsg
          .windowUntil(it -> it.resultSetEnd())
          .map(
              dataRow ->
                  new MariadbResult(
                      false,
                      dataRow,
                      ExceptionFactory.INSTANCE,
                      null,
                      client.getVersion().isMariaDBServer()
                          && client.getVersion().versionGreaterOrEqual(10, 5, 1)));
    }
  }

  @Override
  public MariadbServerParameterizedQueryStatement fetchSize(int rows) {
    return this;
  }

  @Override
  public MariadbServerParameterizedQueryStatement returnGeneratedValues(String... columns) {
    Assert.requireNonNull(columns, "columns must not be null");

    if (!(client.getVersion().isMariaDBServer()
            && client.getVersion().versionGreaterOrEqual(10, 5, 1))
        && columns.length > 1) {
      throw new IllegalArgumentException(
          "returnGeneratedValues can have only one column before MariaDB 10.5.1");
    }
    //    prepareResult.validateAddingReturning();
    this.generatedColumns = columns;
    return this;
  }

  private Flux<org.mariadb.r2dbc.api.MariadbResult> execute(
      String sql, Map<Integer, Parameter<?>> parameters, String[] generatedColumns) {
    ExceptionFactory factory = ExceptionFactory.withSql(sql);

    if (prepareResult == null && client.getPrepareCache() != null) {
      prepareResult = client.getPrepareCache().get(sql);
    }

    Flux<org.mariadb.r2dbc.api.MariadbResult> flux;
    if (prepareResult != null) {
      validateParameters();
      ServerPrepareResult res;
      if (this.client.getPrepareCache() != null
          && (res = this.client.getPrepareCache().get(sql)) != null
          && !res.equals(prepareResult)) {
        prepareResult.decrementUse(client);
        prepareResult = res;
      } else {
        if (!prepareResult.incrementUse()) {
          prepareResult = null;
        }
      }

      if (prepareResult != null) {
        return sendExecuteCmd(factory, parameters, generatedColumns)
            .concatWith(
                Flux.create(
                    sink -> {
                      prepareResult.decrementUse(client);
                      sink.complete();
                      parameters.clear();
                    }));
      }
    }

    if (configuration.allowPipelining()
        && client.getVersion().isMariaDBServer()
        && client.getVersion().versionGreaterOrEqual(10, 2, 0)) {
      flux = sendPrepareAndExecute(sql, factory, parameters, generatedColumns);
    } else {
      flux =
          sendPrepare(sql)
              .flatMapMany(prepareResult1 -> sendExecuteCmd(factory, parameters, generatedColumns));
    }
    return flux.concatWith(
        Flux.create(
            sink -> {
              prepareResult = client.getPrepareCache().get(sql);
              if (prepareResult != null) {
                prepareResult.decrementUse(client);
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
                    dataRow,
                    factory,
                    generatedColumns,
                    client.getVersion().isMariaDBServer()
                        && client.getVersion().versionGreaterOrEqual(10, 5, 1)));
  }

  private Mono<ServerPrepareResult> sendPrepare(String sql) {
    Flux<ServerPrepareResult> f =
        this.client
            .sendCommand(new PreparePacket(sql), DecoderState.PREPARE_RESPONSE, sql)
            .handle(
                (it, sink) -> {
                  if (it instanceof PrepareResultPacket) {
                    PrepareResultPacket packet = (PrepareResultPacket) it;
                    prepareResult =
                        new ServerPrepareResult(
                            packet.getStatementId(), packet.getNumColumns(), packet.getNumParams());
                    if (client.getPrepareCache() != null) {
                      ServerPrepareResult res = client.getPrepareCache().get(sql);
                      if (res != null && !res.equals(prepareResult)) {
                        prepareResult.close(client);
                        prepareResult = res;
                      }
                    }
                    sink.next(prepareResult);
                  }
                  if (it.ending()) sink.complete();
                });
    return f.singleOrEmpty();
  };

  private Flux<org.mariadb.r2dbc.api.MariadbResult> sendExecuteCmd(
      ExceptionFactory factory, Map<Integer, Parameter<?>> parameters, String[] generatedColumns) {
    return this.client
        .sendCommand(
            new ExecutePacket(
                prepareResult != null ? prepareResult.getStatementId() : -1, parameters))
        .windowUntil(it -> it.resultSetEnd())
        .map(
            dataRow ->
                new MariadbResult(
                    false,
                    dataRow,
                    factory,
                    generatedColumns,
                    client.getVersion().isMariaDBServer()
                        && client.getVersion().versionGreaterOrEqual(10, 5, 1)));
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
        + Arrays.toString(generatedColumns)
        + ", prepareResult="
        + prepareResult
        + '}';
  }
}

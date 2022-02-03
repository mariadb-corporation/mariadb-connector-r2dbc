// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.DecoderState;
import org.mariadb.r2dbc.client.MariadbResult;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.codec.ParameterWithCodec;
import org.mariadb.r2dbc.codec.list.StringCodec;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.client.ExecutePacket;
import org.mariadb.r2dbc.message.client.PreparePacket;
import org.mariadb.r2dbc.message.server.CompletePrepareResult;
import org.mariadb.r2dbc.message.server.ErrorPacket;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.MariadbType;
import org.mariadb.r2dbc.util.ServerNamedParamParser;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

final class MariadbServerParameterizedQueryStatement implements MariadbStatement {

  private final Client client;
  private final String initialSql;
  private final MariadbConnectionConfiguration configuration;
  private ServerNamedParamParser paramParser;
  private Map<Integer, ParameterWithCodec> parameters;
  private List<Map<Integer, ParameterWithCodec>> batchingParameters;
  private String[] generatedColumns;
  private AtomicReference<ServerPrepareResult> prepareResult;

  MariadbServerParameterizedQueryStatement(
      Client client, String sql, MariadbConnectionConfiguration configuration) {
    this.client = client;
    this.configuration = configuration;
    this.initialSql = Assert.requireNonNull(sql, "sql must not be null");
    this.parameters = null;
    this.paramParser = null;
    this.prepareResult = new AtomicReference<>(client.getPrepareCache().get(sql));
  }

  @Override
  public MariadbServerParameterizedQueryStatement add() {
    Assert.requireNonNull(this.parameters, "add() cannot be used if not bindings where set");
    validateParameters();
    if (batchingParameters == null) batchingParameters = new ArrayList<>();
    batchingParameters.add(parameters);
    parameters = null;
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
    Assert.requireNonNull(value, "value cannot be null or use bindNull");

    if (parameters == null) parameters = new HashMap<>();
    Parameter paramValue = (value instanceof Parameter) ? (Parameter) value : Parameters.in(value);
    parameters.put(
        index,
        new ParameterWithCodec(
            paramValue,
            paramValue.getValue() != null
                ? Codecs.codecByClass(paramValue.getValue().getClass(), index)
                : StringCodec.INSTANCE));
    return this;
  }

  @Override
  public MariadbServerParameterizedQueryStatement bindNull(
      @Nullable String identifier, @Nullable Class<?> type) {
    Assert.requireNonNull(identifier, "identifier cannot be null");
    return bindNull(getColumn(identifier), type);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public MariadbServerParameterizedQueryStatement bindNull(
      int index, @Nullable Class<?> classType) {
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

    if (parameters == null) parameters = new HashMap<>();
    parameters.put(
        index,
        new ParameterWithCodec(
            (classType == null) ? Parameters.in(MariadbType.VARCHAR) : Parameters.in(classType),
            Codecs.codecByClass((classType == null ? String.class : classType), index)));
    return this;
  }

  private int getColumn(String name) {
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

  private void validateParameters() {
    int paramNumber =
        (prepareResult.get() != null)
            ? prepareResult.get().getNumParams()
            : (((paramParser != null)
                ? paramParser.getParamCount()
                : ServerNamedParamParser.parameterParts(
                        initialSql, this.client.noBackslashEscapes())
                    .getParamCount()));
    if (parameters == null) {
      if (paramNumber > 0) {
        throw new IllegalStateException("No parameter have been set");
      }
    } else {
      // valid parameters
      for (int i = 0; i < paramNumber; i++) {
        if (parameters.get(i) == null) {
          throw new IllegalStateException(String.format("Parameter at position %s is not set", i));
        }
      }
    }
  }

  @Override
  public Flux<org.mariadb.r2dbc.api.MariadbResult> execute() {
    String sql = paramParser == null ? this.initialSql : paramParser.getRealSql();
    if (client.getVersion().supportReturning() && generatedColumns != null) {
      sql +=
          generatedColumns.length == 0
              ? " RETURNING *"
              : " RETURNING " + String.join(", ", generatedColumns);
      prepareResult.set(client.getPrepareCache().get(sql));
    }

    if (batchingParameters == null) {
      validateParameters();
      return executeSingleQuery(sql, this.generatedColumns);
    } else {
      this.add();
      final List<Map<Integer, ParameterWithCodec>> batchParams = new ArrayList<>();
      batchParams.addAll(batchingParameters);

      Flux<ServerMessage> fluxMsg =
          prepareIfNotDone(sql)
              .flatMapMany(
                  prepResult -> {
                    Flux<ServerMessage> flux =
                        this.client.sendCommand(new ExecutePacket(prepResult, batchParams.get(0)));
                    int index = 1;
                    while (index < batchParams.size()) {
                      flux =
                          flux.concatWith(
                              this.client.sendCommand(
                                  new ExecutePacket(prepResult, batchParams.get(index++))));
                    }
                    flux =
                        flux.concatWith(
                            Flux.create(
                                sink -> {
                                  prepResult.decrementUse(client);
                                  sink.complete();
                                }));
                    return flux;
                  });

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

  private Mono<ServerPrepareResult> prepareIfNotDone(String sql) {
    // prepare command, if not already done
    if (prepareResult.get() == null) {
      prepareResult.set(client.getPrepareCache().get(sql));
      if (prepareResult.get() == null) {
        return sendPrepare(sql, ExceptionFactory.withSql(sql));
      }
    }
    return Mono.just(prepareResult.get());
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
      String sql, String[] generatedColumns) {
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
                      parameters = null;
                    }));
      } else {
        // prepare is closing
        prepareResult.set(null);
      }
    }
    if (this.parameters == null) this.parameters = new HashMap<>();

    Flux<org.mariadb.r2dbc.api.MariadbResult> flux;
    if (configuration.allowPipelining()
        && client.getVersion().isMariaDBServer()
        && client.getVersion().versionGreaterOrEqual(10, 2, 0)) {
      flux = sendPrepareAndExecute(sql, factory, parameters, generatedColumns);
    } else {
      flux =
          sendPrepare(sql, factory)
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
              parameters = null;
            }));
  }

  private Flux<org.mariadb.r2dbc.api.MariadbResult> sendPrepareAndExecute(
      String sql,
      ExceptionFactory factory,
      Map<Integer, ParameterWithCodec> parameters,
      String[] generatedColumns) {
    return this.client
        .sendCommand(new PreparePacket(sql), new ExecutePacket(null, parameters))
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

  private Mono<ServerPrepareResult> sendPrepare(String sql, ExceptionFactory factory) {
    Flux<ServerPrepareResult> f =
        this.client
            .sendCommand(new PreparePacket(sql), DecoderState.PREPARE_RESPONSE, sql)
            .handle(
                (it, sink) -> {
                  if (it instanceof ErrorPacket) {
                    sink.error(factory.from((ErrorPacket) it));
                    return;
                  }
                  if (it instanceof CompletePrepareResult) {
                    prepareResult.set(((CompletePrepareResult) it).getPrepare());
                    sink.next(prepareResult.get());
                  }
                  if (it.ending()) sink.complete();
                });
    return f.singleOrEmpty();
  }

  private Flux<org.mariadb.r2dbc.api.MariadbResult> sendExecuteCmd(
      ExceptionFactory factory,
      Map<Integer, ParameterWithCodec> parameters,
      String[] generatedColumns) {
    return this.client
        .sendCommand(new ExecutePacket(prepareResult.get(), parameters))
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

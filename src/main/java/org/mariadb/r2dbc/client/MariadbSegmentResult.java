// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.api.MariadbResult;
import org.mariadb.r2dbc.message.Protocol;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.server.*;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class MariadbSegmentResult extends AbstractReferenceCounted implements MariadbResult {

  private final Flux<Result.Segment> segments;

  private MariadbSegmentResult(Flux<Result.Segment> segments) {
    this.segments = segments;
  }

  MariadbSegmentResult(
      Protocol protocol,
      AtomicReference<ServerPrepareResult> prepareResult,
      Flux<ServerMessage> messages,
      ExceptionFactory factory,
      String[] generatedColumns,
      boolean supportReturning,
      MariadbConnectionConfiguration conf) {

    final List<ColumnDefinitionPacket> columns = new ArrayList<>();
    final AtomicBoolean metaFollows = new AtomicBoolean(true);
    final AtomicReference<MariadbRow.MariadbRowConstructor> rowConstructor =
        new AtomicReference<>();
    final AtomicReference<MariadbRowMetadata> meta = new AtomicReference<>();
    final AtomicBoolean isOutputParameter = new AtomicBoolean();
    this.segments =
        messages.handle(
            (message, sink) -> {
              if (message instanceof CompletePrepareResult) {
                prepareResult.set(((CompletePrepareResult) message).getPrepare());
                return;
              }

              if (message instanceof ColumnCountPacket) {
                metaFollows.set(((ColumnCountPacket) message).isMetaFollows());
                if (!metaFollows.get()) {
                  columns.addAll(Arrays.asList(prepareResult.get().getColumns()));
                }
                return;
              }

              if (message instanceof ColumnDefinitionPacket) {
                columns.add((ColumnDefinitionPacket) message);
                return;
              }

              if (message instanceof EofPacket) {
                EofPacket eof = (EofPacket) message;
                if (!eof.ending()) {
                  rowConstructor.set(
                      protocol == Protocol.TEXT ? MariadbRowText::new : MariadbRowBinary::new);
                  ColumnDefinitionPacket[] columnsArray =
                      columns.toArray(new ColumnDefinitionPacket[0]);

                  meta.set(new MariadbRowMetadata(columnsArray));

                  // in case metadata follows and prepared statement, update meta
                  if (prepareResult != null && prepareResult.get() != null && metaFollows.get()) {
                    prepareResult.get().setColumns(columnsArray);
                  }

                  isOutputParameter.set(
                      (eof.getServerStatus() & ServerStatus.PS_OUT_PARAMETERS) > 0);
                }
                return;
              }

              if (message instanceof ErrorPacket) {
                sink.next(new MariadbErrorSegment((ErrorPacket) message, factory));
                return;
              }

              if (message instanceof OkPacket) {

                if (generatedColumns != null && !supportReturning) {
                  String colName = generatedColumns.length > 0 ? generatedColumns[0] : "ID";
                  MariadbRowMetadata tmpMeta =
                      new MariadbRowMetadata(
                          new ColumnDefinitionPacket[] {
                            ColumnDefinitionPacket.fromGeneratedId(colName, conf)
                          });
                  if (((OkPacket) message).value() > 1) {
                    sink.error(
                        factory.createException(
                            "Connector cannot get generated ID (using returnGeneratedValues)"
                                + " multiple rows before MariaDB 10.5.1",
                            "HY000",
                            -1));
                    return;
                  }
                  ByteBuf buf =
                      org.mariadb.r2dbc.client.MariadbResult.getLongTextEncoded(
                          ((OkPacket) message).getLastInsertId());
                  org.mariadb.r2dbc.api.MariadbRow row = new MariadbRowText(buf, tmpMeta, factory);
                  sink.next(new MariadbRowSegment(row, buf));
                }

                Long rowCount = ((OkPacket) message).value();
                if (rowCount != null) {
                  sink.next(new MariadbUpdateCountSegment(rowCount));
                }
                return;
              }

              if (message instanceof RowPacket) {
                RowPacket row = ((RowPacket) message);
                if (isOutputParameter.get()) {
                  org.mariadb.r2dbc.api.MariadbOutParameters outParameters =
                      new MariadbOutParameters(
                          row.getRaw(), new MariadbOutParametersMetadata(columns), factory);
                  sink.next(new MariadbOutSegment(outParameters, (RowPacket) message));
                } else {
                  org.mariadb.r2dbc.api.MariadbRow rowSegment =
                      rowConstructor.get().create(row.getRaw(), meta.get(), factory);
                  sink.next(new MariadbRowSegment(rowSegment, (RowPacket) message));
                }
              }
            });
  }

  static MariadbSegmentResult toResult(
      Protocol protocol,
      AtomicReference<ServerPrepareResult> prepareResult,
      Flux<ServerMessage> messages,
      ExceptionFactory factory,
      String[] generatedColumns,
      boolean supportReturning,
      MariadbConnectionConfiguration conf) {
    return new MariadbSegmentResult(
        protocol, prepareResult, messages, factory, generatedColumns, supportReturning, conf);
  }

  @Override
  public Mono<Long> getRowsUpdated() {
    return this.segments
        .<Integer>handle(
            (segment, sink) -> {
              try {
                if (segment instanceof MariadbErrorSegment) {
                  sink.error(((MariadbErrorSegment) segment).exception());
                  return;
                }

                if (segment instanceof Result.UpdateCount) {
                  sink.next((int) (((Result.UpdateCount) segment).value()));
                }

              } finally {
                ReferenceCountUtil.release(segment);
              }
            })
        .collectList()
        .handle(
            (list, sink) -> {
              if (list.isEmpty()) {
                return;
              }

              long sum = 0;

              for (Integer integer : list) {
                sum += integer;
              }

              sink.next(sum);
            });
  }

  @Override
  public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
    Assert.requireNonNull(f, "f must not be null");

    return this.segments.handle(
        (segment, sink) -> {
          try {
            if (segment instanceof MariadbErrorSegment) {
              sink.error(((MariadbErrorSegment) segment).exception());
              return;
            }

            if (segment instanceof Result.RowSegment) {
              Result.RowSegment row = (Result.RowSegment) segment;
              sink.next(f.apply(row.row(), row.row().getMetadata()));
            }

          } finally {
            ReferenceCountUtil.release(segment);
          }
        });
  }

  @Override
  public MariadbSegmentResult filter(Predicate<Result.Segment> filter) {
    Assert.requireNonNull(filter, "filter must not be null");
    return new MariadbSegmentResult(
        this.segments.filter(
            it -> {
              boolean result = filter.test(it);
              if (!result) {
                ReferenceCountUtil.release(it);
              }
              return result;
            }));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Publisher<T> flatMap(
      Function<Result.Segment, ? extends Publisher<? extends T>> mappingFunction) {
    Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
    return this.segments.concatMap(
        segment -> {
          Publisher<? extends T> result = mappingFunction.apply(segment);
          if (result == null) {
            return Mono.error(new IllegalStateException("The mapper returned a null Publisher"));
          }

          if (result instanceof Mono) {
            return ((Mono<T>) result).doFinally(s -> ReferenceCountUtil.release(segment));
          }

          return Flux.from(result).doFinally(s -> ReferenceCountUtil.release(segment));
        });
  }

  @Override
  protected void deallocate() {
    this.getRowsUpdated().subscribe();
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    return this;
  }

  @Override
  public String toString() {
    return "MariadbSegmentResult{segments=" + this.segments + '}';
  }

  static class MariadbRowSegment extends AbstractReferenceCounted implements Result.RowSegment {
    private final Row row;
    private final ReferenceCounted releaseable;

    public MariadbRowSegment(Row row, ReferenceCounted releaseable) {
      this.row = row;
      this.releaseable = releaseable;
    }

    @Override
    public Row row() {
      return this.row;
    }

    @Override
    protected void deallocate() {
      ReferenceCountUtil.release(this.releaseable);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
      return this;
    }
  }

  static class MariadbOutSegment extends AbstractReferenceCounted implements Result.OutSegment {

    private final OutParameters outParameters;

    private final ReferenceCounted releaseable;

    public MariadbOutSegment(OutParameters outParameters, ReferenceCounted releaseable) {
      this.outParameters = outParameters;
      this.releaseable = releaseable;
    }

    @Override
    public OutParameters outParameters() {
      return outParameters;
    }

    @Override
    protected void deallocate() {
      ReferenceCountUtil.release(this.releaseable);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
      return this;
    }
  }

  static class MariadbUpdateCountSegment implements Result.UpdateCount {

    private final long value;

    public MariadbUpdateCountSegment(long value) {
      this.value = value;
    }

    @Override
    public long value() {
      return this.value;
    }
  }

  static class MariadbErrorSegment implements Result.Message {

    private final ExceptionFactory factory;

    private final ErrorPacket error;

    public MariadbErrorSegment(ErrorPacket error, ExceptionFactory factory) {
      this.factory = factory;
      this.error = error;
    }

    @Override
    public R2dbcException exception() {
      return this.factory.from(error);
    }

    @Override
    public int errorCode() {
      return error.errorCode();
    }

    @Override
    public String sqlState() {
      return error.sqlState();
    }

    @Override
    public String message() {
      return error.getMessage();
    }
  }
}

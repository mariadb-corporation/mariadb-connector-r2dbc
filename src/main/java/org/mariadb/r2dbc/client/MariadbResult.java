// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.codec.BinaryRowDecoder;
import org.mariadb.r2dbc.codec.RowDecoder;
import org.mariadb.r2dbc.codec.TextRowDecoder;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.server.*;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

public final class MariadbResult implements org.mariadb.r2dbc.api.MariadbResult {

  private final Flux<ServerMessage> dataRows;
  private final ExceptionFactory factory;

  private final String[] generatedColumns;
  private final boolean supportReturning;
  private final boolean text;
  private final MariadbConnectionConfiguration conf;
  private AtomicReference<ServerPrepareResult> prepareResult;
  private Predicate<Segment> filter;

  private volatile MariadbDataSegment segment;

  public MariadbResult(
      boolean text,
      AtomicReference<ServerPrepareResult> prepareResult,
      Flux<ServerMessage> dataRows,
      ExceptionFactory factory,
      String[] generatedColumns,
      boolean supportReturning,
      MariadbConnectionConfiguration conf) {
    this.text = text;
    this.dataRows = dataRows;
    this.factory = factory;
    this.generatedColumns = generatedColumns;
    this.supportReturning = supportReturning;
    this.conf = conf;
    this.prepareResult = prepareResult;
    this.filter = null;
  }

  @Override
  public Flux<Integer> getRowsUpdated() {
    // Since CLIENT_DEPRECATE_EOF is not set in order to identify output parameter
    // number of updated row can be identified either by OK_Packet or number of rows in case of
    // RETURNING
    final AtomicInteger rowCount = new AtomicInteger(0);
    return this.dataRows
        .takeUntil(ServerMessage::resultSetEnd)
        .handle(
            (serverMessage, sink) -> {
              if (serverMessage instanceof ErrorPacket) {
                sink.error(this.factory.from((ErrorPacket) serverMessage));
                return;
              }

              if (serverMessage instanceof OkPacket) {
                OkPacket okPacket = ((OkPacket) serverMessage);
                sink.next((int) okPacket.value());
                sink.complete();
                return;
              }

              if (serverMessage instanceof EofPacket) {
                EofPacket eofPacket = ((EofPacket) serverMessage);
                if (eofPacket.resultSetEnd()) {
                  sink.next(rowCount.get());
                  rowCount.set(0);
                  sink.complete();
                }
                return;
              }

              if (serverMessage instanceof RowPacket) {
                rowCount.incrementAndGet();
                ((RowPacket) serverMessage).release();
                return;
              }
            });
  }

  public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
    Flux<Result.Segment> flux =
        this.dataRows
            .takeUntil(ServerMessage::resultSetEnd)
            .handle(this.handler(true))
            .filter(MariadbRowSegment.class::isInstance);
    if (filter != null) flux = flux.filter(filter);

    return flux.cast(MariadbRowSegment.class)
        .map(
            it -> {
              try {
                return mappingFunction.apply(it.row(), it.getMetadata());
              } catch (IllegalArgumentException i) {
                throw this.factory.createException(i.getMessage(), "HY000", -1);
              }
            });
  }

  @Override
  public <T> Flux<T> map(Function<? super Readable, ? extends T> mappingFunction) {
    Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
    Flux<Result.Segment> flux =
        this.dataRows.takeUntil(ServerMessage::resultSetEnd).handle(this.handler(true));
    if (filter != null) flux = flux.filter(filter);

    return flux.cast(MariadbRowSegment.class).map(it -> mappingFunction.apply(it.row()));
  }

  @Override
  public Result filter(Predicate<Segment> filter) {
    this.filter = filter;
    return this;
  }

  @Override
  public <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
    Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
    Flux<Result.Segment> flux =
        this.dataRows.takeUntil(ServerMessage::resultSetEnd).handle(this.handler(true));
    if (filter != null) flux = flux.filter(filter);
    return flux.flatMap(it -> mappingFunction.apply(it));
  }

  private BiConsumer<? super ServerMessage, SynchronousSink<Segment>> handler(boolean throwError) {
    final List<ColumnDefinitionPacket> columns = new ArrayList<>();
    return (serverMessage, sink) -> {
      if (serverMessage instanceof ErrorPacket) {
        if (throwError) {
          sink.error(this.factory.from((ErrorPacket) serverMessage));
        } else {
          sink.next((ErrorPacket) serverMessage);
          sink.complete();
        }
        return;
      }

      if (serverMessage instanceof CompletePrepareResult) {
        this.prepareResult.set(((CompletePrepareResult) serverMessage).getPrepare());
        return;
      }

      if (serverMessage instanceof ColumnCountPacket) {
        if (!((ColumnCountPacket) serverMessage).isMetaFollows()) {
          columns.addAll(Arrays.asList(this.prepareResult.get().getColumns()));
        }
        return;
      }

      if (serverMessage instanceof ColumnDefinitionPacket) {
        columns.add((ColumnDefinitionPacket) serverMessage);
        return;
      }

      if (serverMessage instanceof OkPacket) {
        OkPacket okPacket = ((OkPacket) serverMessage);
        // This is for server that doesn't permit RETURNING: rely on OK_packet LastInsertId
        // to retrieve the last generated ID.
        if (generatedColumns != null && !supportReturning && serverMessage instanceof OkPacket) {
          String colName = generatedColumns.length > 0 ? generatedColumns[0] : "ID";
          List<ColumnDefinitionPacket> tmpCol =
              Collections.singletonList(ColumnDefinitionPacket.fromGeneratedId(colName, conf));
          if (okPacket.value() > 1) {
            sink.error(
                this.factory.createException(
                    "Connector cannot get generated ID (using returnGeneratedValues) multiple rows before MariaDB 10.5.1",
                    "HY000",
                    -1));
            return;
          }

          ByteBuf buf = getLongTextEncoded(okPacket.getLastInsertId());
          segment =
              new MariadbRowSegment(new TextRowDecoder(tmpCol, this.conf, this.factory), tmpCol);
          segment.updateRaw(buf);
          sink.next(segment);
        } else sink.next(okPacket);
        return;
      }

      if (serverMessage instanceof EofPacket) {
        RowDecoder decoder =
            text
                ? new TextRowDecoder(columns, this.conf, this.factory)
                : new BinaryRowDecoder(columns, this.conf, this.factory);
        boolean outputParameter =
            (((EofPacket) serverMessage).getServerStatus() & ServerStatus.PS_OUT_PARAMETERS) > 0;
        segment =
            outputParameter
                ? new MariadbOutSegment(decoder, columns)
                : new MariadbRowSegment(decoder, columns);
        return;
      }

      if (serverMessage instanceof RowPacket) {
        RowPacket row = ((RowPacket) serverMessage);
        try {
          segment.updateRaw(row.getRaw());
          sink.next(segment);
        } catch (IllegalArgumentException i) {
          sink.error(this.factory.createException(i.getMessage(), "HY000", -1));
        } catch (R2dbcException i) {
          sink.error(i);
        } finally {
          row.release();
        }
        return;
      }
    };
  }

  private ByteBuf getLongTextEncoded(long value) {
    byte[] byteValue = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    byte[] encodedLength;
    int length = byteValue.length;
    encodedLength = new byte[] {(byte) length};
    return Unpooled.copiedBuffer(encodedLength, byteValue);
  }
}

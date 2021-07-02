// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.mariadb.r2dbc.codec.BinaryRowDecoder;
import org.mariadb.r2dbc.codec.RowDecoder;
import org.mariadb.r2dbc.codec.TextRowDecoder;
import org.mariadb.r2dbc.message.server.*;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

final class MariadbResult implements org.mariadb.r2dbc.api.MariadbResult {

  private final Flux<ServerMessage> dataRows;
  private final ExceptionFactory factory;
  private RowDecoder decoder;
  private final String[] generatedColumns;
  private final boolean supportReturning;
  private final boolean text;
  private final MariadbConnectionConfiguration conf;
  private AtomicReference<ServerPrepareResult> prepareResult;

  private volatile ColumnDefinitionPacket[] metadataList;
  private volatile int metadataIndex;
  private volatile int columnNumber;
  private volatile MariadbRowMetadata rowMetadata;

  MariadbResult(
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
  }

  @Override
  public Mono<Integer> getRowsUpdated() {
    Flux<Integer> f =
        this.dataRows.handle(
            (serverMessage, sink) -> {
              if (serverMessage instanceof ErrorPacket) {
                sink.error(this.factory.from((ErrorPacket) serverMessage));
                return;
              }

              if (serverMessage instanceof OkPacket) {
                OkPacket okPacket = (OkPacket) serverMessage;
                long affectedRows = okPacket.getAffectedRows();
                sink.next((int) affectedRows);
                sink.complete();
              }
            });
    return f.singleOrEmpty();
  }

  @Override
  public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
    metadataIndex = 0;

    return this.dataRows
        .takeUntil(msg -> msg.resultSetEnd())
        .handle(
            (serverMessage, sink) -> {
              if (serverMessage instanceof ErrorPacket) {
                sink.error(this.factory.from((ErrorPacket) serverMessage));
                return;
              }

              if (serverMessage instanceof CompletePrepareResult) {
                this.prepareResult.set(((CompletePrepareResult) serverMessage).getPrepare());
                metadataList = this.prepareResult.get().getColumns();
                return;
              }

              if (serverMessage instanceof ColumnCountPacket) {
                this.columnNumber = ((ColumnCountPacket) serverMessage).getColumnCount();
                if (!((ColumnCountPacket) serverMessage).isMetaFollows()) {
                  metadataList = this.prepareResult.get().getColumns();
                  rowMetadata = MariadbRowMetadata.toRowMetadata(this.metadataList);
                  this.decoder = new BinaryRowDecoder(columnNumber, this.metadataList, this.conf);
                } else {
                  metadataList = new ColumnDefinitionPacket[this.columnNumber];
                }
                return;
              }

              if (serverMessage instanceof ColumnDefinitionPacket) {
                this.metadataList[metadataIndex++] = (ColumnDefinitionPacket) serverMessage;
                if (metadataIndex == columnNumber) {
                  rowMetadata = MariadbRowMetadata.toRowMetadata(this.metadataList);
                  this.decoder =
                      text
                          ? new TextRowDecoder(columnNumber, this.metadataList, this.conf)
                          : new BinaryRowDecoder(columnNumber, this.metadataList, this.conf);
                }
                return;
              }

              if (serverMessage instanceof RowPacket) {
                ByteBuf buf = ((RowPacket) serverMessage).getRaw();
                try {
                  sink.next(f.apply(new MariadbRow(metadataList, decoder, buf), rowMetadata));
                } catch (IllegalArgumentException i) {
                  sink.error(this.factory.createException(i.getMessage(), "HY000", -1));
                } finally {
                  buf.release();
                }
                return;
              }

              // This is for server that doesn't permit RETURNING: rely on OK_packet LastInsertId
              // to retrieve the last generated ID.
              if (generatedColumns != null
                  && !supportReturning
                  && serverMessage instanceof OkPacket) {

                String colName = generatedColumns.length > 0 ? generatedColumns[0] : "ID";
                metadataList = new ColumnDefinitionPacket[1];
                metadataList[0] = ColumnDefinitionPacket.fromGeneratedId(colName);
                rowMetadata = MariadbRowMetadata.toRowMetadata(this.metadataList);

                OkPacket okPacket = ((OkPacket) serverMessage);
                if (okPacket.getAffectedRows() > 1) {
                  sink.error(
                      this.factory.createException(
                          "Connector cannot get generated ID (using returnGeneratedValues) multiple rows before MariaDB 10.5.1",
                          "HY000",
                          -1));
                  return;
                }
                ByteBuf buf = getLongTextEncoded(okPacket.getLastInsertId());
                decoder = new TextRowDecoder(1, this.metadataList, this.conf);
                try {
                  sink.next(f.apply(new MariadbRow(metadataList, decoder, buf), rowMetadata));
                } finally {
                  buf.release();
                }
              }
            });
  }

  private ByteBuf getLongTextEncoded(long value) {
    byte[] byteValue = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    byte[] encodedLength;
    int length = byteValue.length;
    encodedLength = new byte[] {(byte) length};
    return Unpooled.copiedBuffer(encodedLength, byteValue);
  }
}

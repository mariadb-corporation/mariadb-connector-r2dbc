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
import io.netty.buffer.Unpooled;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.mariadb.r2dbc.codec.RowDecoder;
import org.mariadb.r2dbc.codec.TextRowDecoder;
import org.mariadb.r2dbc.message.server.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;

final class MariadbResult implements org.mariadb.r2dbc.api.MariadbResult {

  private final Flux<ServerMessage> dataRows;
  private final ExceptionFactory factory;
  private final RowDecoder decoder;
  private final String[] generatedColumns;
  private final boolean supportReturning;

  private volatile ColumnDefinitionPacket[] metadataList;
  private volatile int metadataIndex;
  private volatile int columnNumber;
  private volatile MariadbRowMetadata rowMetadata;

  MariadbResult(
      boolean text,
      Flux<ServerMessage> dataRows,
      ExceptionFactory factory,
      String[] generatedColumns,
      boolean supportReturning) {
    this.dataRows = dataRows;
    this.factory = factory;
    // TODO do binary decoder too
    this.decoder = new TextRowDecoder();
    this.generatedColumns = generatedColumns;
    this.supportReturning = supportReturning;
  }

  @Override
  public Mono<Integer> getRowsUpdated() {
    return this.dataRows
        .singleOrEmpty()
        .handle(
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

              if (serverMessage instanceof RowPacket) {
                // in case of having a resultset
                ((RowPacket) serverMessage).getRaw().release();
              }
            });
  }

  @Override
  public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
    metadataIndex = 0;
    return this.dataRows.handle(
        (serverMessage, sink) -> {
          if (serverMessage instanceof ErrorPacket) {
            sink.error(this.factory.from((ErrorPacket) serverMessage));
            return;
          }

          if (serverMessage instanceof ColumnCountPacket) {
            this.columnNumber = ((ColumnCountPacket) serverMessage).getColumnCount();
            metadataList = new ColumnDefinitionPacket[this.columnNumber];
            return;
          }

          if (serverMessage instanceof ColumnDefinitionPacket) {
            this.metadataList[metadataIndex++] = (ColumnDefinitionPacket) serverMessage;
            if (metadataIndex == columnNumber) {
              rowMetadata = MariadbRowMetadata.toRowMetadata(this.metadataList);
            }
            return;
          }

          if (serverMessage instanceof RowPacket) {
            ByteBuf buf = ((RowPacket) serverMessage).getRaw();
            try {
              sink.next(f.apply(new MariadbRow(metadataList, decoder, buf), rowMetadata));
              return;
            } catch (IllegalArgumentException i) {
              // in case parsing isn't possible for datatype
              sink.error(this.factory.createException(i.getMessage(), "HY000", -1));
            }
          }

          // This is for server that doesn't permit RETURNING: rely on OK_packet LastInsertId
          // to retrieve the last generated ID.
          if (serverMessage instanceof OkPacket && generatedColumns != null && !supportReturning) {
            if (metadataList == null) {
              String colName = generatedColumns.length > 0 ? generatedColumns[0] : "ID";
              metadataList = new ColumnDefinitionPacket[1];
              metadataList[0] = ColumnDefinitionPacket.fromGeneratedId(colName);
              rowMetadata = MariadbRowMetadata.toRowMetadata(this.metadataList);
            }
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
            sink.next(f.apply(new MariadbRow(metadataList, decoder, buf), rowMetadata));
          }

          if (serverMessage.resultSetEnd()) {
            sink.complete();
          }
        });
  }

  private ByteBuf getLongTextEncoded(long value) {
    byte[] byteValue = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    byte[] encodedLength;
    int length = byteValue.length;
    if (length < 251) {
      encodedLength = new byte[] {(byte) length};
    } else if (length < 65536) {
      encodedLength = new byte[] {(byte) 0xfc, (byte) length, (byte) (length >>> 8)};
    } else if (length < 16777216) {
      encodedLength =
          new byte[] {(byte) 0xfd, (byte) length, (byte) (length >>> 8), (byte) (length >>> 16)};
    } else {
      encodedLength =
          new byte[] {
            (byte) 0xfd,
            (byte) length,
            (byte) (length >>> 8),
            (byte) (length >>> 16),
            (byte) (length >>> 24),
            (byte) (length >>> 32),
            (byte) (length >>> 40),
            (byte) (length >>> 48),
            (byte) (length >>> 56)
          };
    }
    return Unpooled.copiedBuffer(encodedLength, byteValue);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("MariadbResult{messages=");
    sb.append(dataRows).append(", factory=").append(factory).append(", metadataList=[");
    if (metadataList == null) {
      sb.append("null");
    } else {
      for (ColumnDefinitionPacket packet : metadataList) {
        sb.append(packet).append(",");
      }
    }
    sb.append("], columnNumber=")
        .append(columnNumber)
        .append(", rowMetadata=")
        .append(rowMetadata)
        .append("}");
    return sb.toString();
  }
}

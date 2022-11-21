package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.server.*;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MariadbResult extends AbstractReferenceCounted
    implements org.mariadb.r2dbc.api.MariadbResult {

  private final boolean protocolType;

  private final Flux<ServerMessage> messages;

  private final ExceptionFactory factory;

  private final String[] generatedColumns;
  private final boolean supportReturning;
  private final MariadbConnectionConfiguration conf;
  private final AtomicReference<ServerPrepareResult> prepareResult;

  public MariadbResult(
      boolean text,
      AtomicReference<ServerPrepareResult> prepareResult,
      Flux<ServerMessage> messages,
      ExceptionFactory factory,
      String[] generatedColumns,
      boolean supportReturning,
      MariadbConnectionConfiguration conf) {
    this.protocolType = text;
    this.messages = messages;
    this.factory = factory;
    this.generatedColumns = generatedColumns;
    this.supportReturning = supportReturning;
    this.conf = conf;
    this.prepareResult = prepareResult;
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public Mono<Long> getRowsUpdated() {
    // Since CLIENT_DEPRECATE_EOF is not set in order to identify output parameter
    // number of updated row can be identified either by OK_Packet or number of rows in case of
    // RETURNING
    final AtomicLong rowCount = new AtomicLong(0);
    return this.messages
        .<Long>handle(
            (serverMessage, sink) -> {
              if (serverMessage instanceof ErrorPacket) {
                sink.error(this.factory.from((ErrorPacket) serverMessage));
                return;
              }

              if (serverMessage instanceof OkPacket) {
                OkPacket okPacket = ((OkPacket) serverMessage);
                sink.next(okPacket.value());
                return;
              }

              if (serverMessage instanceof EofPacket) {
                EofPacket eofPacket = ((EofPacket) serverMessage);
                if (eofPacket.resultSetEnd()) {
                  sink.next(rowCount.get());
                  rowCount.set(0);
                }
                return;
              }

              if (serverMessage instanceof RowPacket) {
                rowCount.incrementAndGet();
                serverMessage.release();
              }
            })
        .collectList()
        .handle(
            (list, sink) -> {
              if (list.isEmpty()) {
                return;
              }

              long sum = 0;

              for (Long i : list) {
                sum += i;
              }

              sink.next(sum);
              sink.complete();
            });
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
    final List<ColumnDefinitionPacket> columns = new ArrayList<>();
    final AtomicBoolean metaFollows = new AtomicBoolean(true);
    final AtomicReference<MariadbRow.MariadbRowConstructor> rowConstructor =
        new AtomicReference<>();
    final AtomicReference<MariadbRowMetadata> meta = new AtomicReference<>();

    return this.messages.handle(
        (message, sink) -> {
          if (message instanceof ErrorPacket) {
            sink.error(this.factory.from((ErrorPacket) message));
            return;
          }
          if (message instanceof CompletePrepareResult) {
            this.prepareResult.set(((CompletePrepareResult) message).getPrepare());
            return;
          }

          if (message instanceof ColumnCountPacket) {
            metaFollows.set(((ColumnCountPacket) message).isMetaFollows());
            if (!metaFollows.get()) {
              columns.addAll(Arrays.asList(this.prepareResult.get().getColumns()));
            }
            return;
          }

          if (message instanceof OkPacket) {
            OkPacket okPacket = ((OkPacket) message);
            // This is for server that doesn't permit RETURNING: rely on OK_packet LastInsertId
            // to retrieve the last generated ID.
            if (generatedColumns != null && !supportReturning) {
              String colName = generatedColumns.length > 0 ? generatedColumns[0] : "ID";
              MariadbRowMetadata tmpMeta =
                  new MariadbRowMetadata(
                      Collections.singletonList(
                          ColumnDefinitionPacket.fromGeneratedId(colName, conf)));
              if (okPacket.value() > 1) {
                sink.error(
                    this.factory.createException(
                        "Connector cannot get generated ID (using returnGeneratedValues) multiple rows before MariaDB 10.5.1",
                        "HY000",
                        -1));
                return;
              }

              ByteBuf buf = getLongTextEncoded(okPacket.getLastInsertId());
              org.mariadb.r2dbc.api.MariadbRow row = new MariadbRowText(buf, tmpMeta, factory);
              sink.next(f.apply(row, meta.get()));
            }
            return;
          }

          if (message instanceof ColumnDefinitionPacket) {
            columns.add((ColumnDefinitionPacket) message);
            return;
          }

          if (message instanceof EofPacket) {
            rowConstructor.set(protocolType ? MariadbRowText::new : MariadbRowBinary::new);
            meta.set(new MariadbRowMetadata(columns));
            boolean outputParameter =
                (((EofPacket) message).getServerStatus() & ServerStatus.PS_OUT_PARAMETERS) > 0;
            // in case metadata follows and prepared statement, update meta
            if (prepareResult != null && prepareResult.get() != null && metaFollows.get()) {
              prepareResult.get().setColumns(columns.toArray(new ColumnDefinitionPacket[0]));
            }
            return;
          }

          if (message instanceof RowPacket) {
            try {
              org.mariadb.r2dbc.api.MariadbRow row =
                  rowConstructor.get().create(((RowPacket) message).getRaw(), meta.get(), factory);
              sink.next(f.apply(row, meta.get()));
            } finally {
              ReferenceCountUtil.release(message);
            }
          }
        });
  }

  public static ByteBuf getLongTextEncoded(long value) {
    byte[] byteValue = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    byte[] encodedLength;
    int length = byteValue.length;
    encodedLength = new byte[] {(byte) length};
    return Unpooled.copiedBuffer(encodedLength, byteValue);
  }

  @Override
  public org.mariadb.r2dbc.api.MariadbResult filter(Predicate<Segment> filter) {
    return MariadbSegmentResult.toResult(
            protocolType,
            prepareResult,
            messages,
            factory,
            generatedColumns,
            supportReturning,
            conf)
        .filter(filter);
  }

  @Override
  public <T> Publisher<T> flatMap(
      Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
    return MariadbSegmentResult.toResult(
            protocolType,
            prepareResult,
            messages,
            factory,
            generatedColumns,
            supportReturning,
            conf)
        .flatMap(mappingFunction);
  }

  @Override
  protected void deallocate() {

    // drain messages for cleanup
    this.getRowsUpdated().subscribe();
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    return this;
  }

  @Override
  public String toString() {
    return "MariadbResult{}";
  }
}

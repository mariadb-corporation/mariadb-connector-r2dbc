// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.BindValue;
import org.mariadb.r2dbc.util.ClientParser;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class QueryWithParametersPacket implements ClientMessage {

  private final ClientParser parser;
  private final BindValue[] bindValues;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);
  private final String[] generatedColumns;
  private ByteBuf savedBuf = null;

  public QueryWithParametersPacket(
      ClientParser parser, BindValue[] bindValues, String[] generatedColumns) {
    this.parser = parser;
    this.bindValues = bindValues;
    this.generatedColumns = generatedColumns;
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator byteBufAllocator) {
    if (savedBuf != null) {
      ByteBuf tmp = savedBuf;
      this.savedBuf = null;
      return Mono.just(tmp);
    }

    Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    final String additionalReturningPart =
        (generatedColumns != null)
            ? (generatedColumns.length == 0
                ? " RETURNING *"
                : " RETURNING " + String.join(", ", generatedColumns))
            : null;

    ByteBuf out = byteBufAllocator.ioBuffer();
    out.writeByte(0x03);

    if (parser.getParamCount() == 0) {
      out.writeBytes(parser.getQuery());
      if (additionalReturningPart != null)
        out.writeCharSequence(additionalReturningPart, StandardCharsets.UTF_8);
    } else {

      // check that some parameters need Mono
      boolean direct = true;
      for (int i = 0; i < parser.getParamCount(); i++) {
        BindValue param = bindValues[i];
        if (!param.getCodec().isDirect()) {
          direct = false;
          break;
        }
      }

      if (direct) {
        int currentPos = 0;
        for (int i = 0; i < parser.getParamCount(); i++) {
          out.writeBytes(
              parser.getQuery(), currentPos, parser.getParamPositions().get(i * 2) - currentPos);
          currentPos = parser.getParamPositions().get(i * 2 + 1);
          BindValue param = bindValues[i];
          if (param.getValue() == null) {
            out.writeBytes("null".getBytes(StandardCharsets.US_ASCII));
          } else {
            param.encodeDirectText(out, context);
          }
        }
        if (currentPos < parser.getQuery().length) {
          out.writeBytes(parser.getQuery(), currentPos, parser.getQuery().length - currentPos);
        }
        if (additionalReturningPart != null)
          out.writeCharSequence(additionalReturningPart, StandardCharsets.UTF_8);
      } else {

        AtomicInteger currentPos = new AtomicInteger(0);
        return Flux.range(0, parser.getParamCount())
            .flatMap(
                i -> {
                  out.writeBytes(
                      parser.getQuery(),
                      currentPos.get(),
                      parser.getParamPositions().get(i * 2) - currentPos.get());
                  currentPos.set(parser.getParamPositions().get(i * 2 + 1));
                  BindValue param = bindValues[i];
                  if (param.getValue() == null) {
                    out.writeBytes("null".getBytes(StandardCharsets.US_ASCII));
                  } else if (param.getCodec().isDirect()) {
                    param.encodeDirectText(out, context);
                  } else {
                    return param
                        .encodeText(byteBufAllocator, context)
                        .map(
                            b -> {
                              out.writeBytes(b);
                              b.release();
                              return Mono.empty();
                            });
                  }
                  return Mono.empty();
                })
            .then()
            .doOnSuccess(
                v -> {
                  if (currentPos.get() < parser.getQuery().length) {
                    out.writeBytes(
                        parser.getQuery(),
                        currentPos.get(),
                        parser.getQuery().length - currentPos.get());
                  }
                  if (additionalReturningPart != null)
                    out.writeCharSequence(additionalReturningPart, StandardCharsets.UTF_8);
                })
            .then(Mono.just(out));
      }
    }
    return Mono.just(out);
  }

  public void save(ByteBuf buf, int initialReaderIndex) {
    savedBuf = buf.readerIndex(initialReaderIndex).retain();
  }

  public void resetSequencer() {
    sequencer.reset();
  }

  public void releaseSave() {
    if (savedBuf != null) {
      savedBuf.release();
      savedBuf = null;
    }
  }

  public MessageSequence getSequencer() {
    return sequencer;
  }
}

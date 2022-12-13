// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.BindEncodedValue;
import org.mariadb.r2dbc.util.ClientParser;

public final class QueryWithParametersPacket implements ClientMessage {

  private final ClientParser parser;
  private final List<BindEncodedValue> bindValues;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);
  private final String[] generatedColumns;
  private ByteBuf savedBuf = null;

  public QueryWithParametersPacket(
      ClientParser parser, List<BindEncodedValue> bindValues, String[] generatedColumns) {
    this.parser = parser;
    this.bindValues = bindValues;
    this.generatedColumns = generatedColumns;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator byteBufAllocator) {
    if (savedBuf != null) {
      ByteBuf tmp = savedBuf;
      this.savedBuf = null;
      return tmp;
    }

    Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    String additionalReturningPart = null;
    if (generatedColumns != null) {
      additionalReturningPart =
          generatedColumns.length == 0
              ? " RETURNING *"
              : " RETURNING " + String.join(", ", generatedColumns);
    }

    ByteBuf out = byteBufAllocator.ioBuffer();
    out.writeByte(0x03);

    if (parser.getParamCount() == 0) {
      out.writeBytes(parser.getQuery());
      if (additionalReturningPart != null)
        out.writeCharSequence(additionalReturningPart, StandardCharsets.UTF_8);
    } else {
      int currentPos = 0;
      for (int i = 0; i < parser.getParamCount(); i++) {
        out.writeBytes(
            parser.getQuery(), currentPos, parser.getParamPositions().get(i * 2) - currentPos);
        currentPos = parser.getParamPositions().get(i * 2 + 1);
        BindEncodedValue param = bindValues.get(i);
        if (param.getValue() == null) {
          out.writeBytes("null".getBytes(StandardCharsets.US_ASCII));
        } else {
          out.writeBytes(param.getValue());
        }
      }
      if (currentPos < parser.getQuery().length) {
        out.writeBytes(parser.getQuery(), currentPos, parser.getQuery().length - currentPos);
      }
      if (additionalReturningPart != null)
        out.writeCharSequence(additionalReturningPart, StandardCharsets.UTF_8);
    }
    return out;
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

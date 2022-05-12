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
import org.mariadb.r2dbc.util.ClientPrepareResult;

public final class QueryWithParametersPacket implements ClientMessage {

  private final ClientPrepareResult prepareResult;
  private final List<BindEncodedValue> bindValues;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);
  private final String[] generatedColumns;
  private ByteBuf savedBuf = null;

  public QueryWithParametersPacket(
      ClientPrepareResult prepareResult,
      List<BindEncodedValue> bindValues,
      String[] generatedColumns) {
    this.prepareResult = prepareResult;
    this.bindValues = bindValues;
    this.generatedColumns = generatedColumns;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator byteBufAllocator) {
    if (savedBuf != null) return savedBuf;
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

    if (prepareResult.getParamCount() == 0) {
      out.writeBytes(prepareResult.getQueryParts().get(0));
      if (additionalReturningPart != null)
        out.writeCharSequence(additionalReturningPart, StandardCharsets.UTF_8);
    } else {
      out.writeBytes(prepareResult.getQueryParts().get(0));
      for (int i = 0; i < prepareResult.getParamCount(); i++) {
        BindEncodedValue param = bindValues.get(i);
        if (param.getValue() == null) {
          out.writeBytes("null".getBytes(StandardCharsets.US_ASCII));
        } else {
          out.writeBytes(param.getValue());
        }
        out.writeBytes(prepareResult.getQueryParts().get(i + 1));
      }
      if (additionalReturningPart != null)
        out.writeCharSequence(additionalReturningPart, StandardCharsets.UTF_8);
    }
    return out;
  }

  public void save(ByteBuf buf, int initialReaderIndex) {
    savedBuf = buf.readerIndex(initialReaderIndex).retain();
  }

  @Override
  public void releaseEncodedBinds() {
    bindValues.forEach(
        b -> {
          if (b.getValue() != null) b.getValue().release();
        });
    bindValues.clear();
  }

  public void resetSequencer() {
    sequencer.reset();
  }

  public MessageSequence getSequencer() {
    return sequencer;
  }
}

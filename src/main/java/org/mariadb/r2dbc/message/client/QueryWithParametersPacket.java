// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.ParameterWithCodec;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ClientPrepareResult;

public final class QueryWithParametersPacket implements ClientMessage {

  private final ClientPrepareResult prepareResult;
  private final ParameterWithCodec[] parameters;
  private final String[] generatedColumns;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);
  private final ExceptionFactory factory;

  public QueryWithParametersPacket(
      ClientPrepareResult prepareResult,
      ParameterWithCodec[] parameters,
      String[] generatedColumns,
      ExceptionFactory factory) {
    this.prepareResult = prepareResult;
    this.parameters = parameters;
    this.generatedColumns = generatedColumns;
    this.factory = factory;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator byteBufAllocator) {
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
        if (parameters[i].getValue() == null) {
          out.writeBytes("null".getBytes(StandardCharsets.US_ASCII));
        } else {
          parameters[i].getCodec().encodeText(out, context, parameters[i].getValue(), factory);
        }
        out.writeBytes(prepareResult.getQueryParts().get(i + 1));
      }
      if (additionalReturningPart != null)
        out.writeCharSequence(additionalReturningPart, StandardCharsets.UTF_8);
    }
    return out;
  }

  public MessageSequence getSequencer() {
    return sequencer;
  }
}

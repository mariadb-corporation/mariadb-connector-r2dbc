// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.Parameter;
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.codec.list.StringCodec;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ClientPrepareResult;

public final class QueryWithParametersPacket implements ClientMessage {

  private final ClientPrepareResult prepareResult;
  private final Parameter[] parameters;
  private final Codec<?>[] codecs;
  private final String[] generatedColumns;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);

  public QueryWithParametersPacket(
      ClientPrepareResult prepareResult, Parameter[] parameters, String[] generatedColumns) {
    this.prepareResult = prepareResult;

    this.parameters = parameters;
    this.codecs = new Codec<?>[parameters.length];

    for (int i = 0; i < parameters.length; i++) {
      Parameter p = parameters[i];
      if (p instanceof Parameter.In) {
        if (p.getValue() == null) {
          codecs[i] = StringCodec.INSTANCE;
        } else {
          codecs[i] = Codecs.codecByClass(p.getValue().getClass(), i);
        }
      }
    }
    this.generatedColumns = generatedColumns;
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
          codecs[i].encodeText(out, context, parameters[i].getValue());
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

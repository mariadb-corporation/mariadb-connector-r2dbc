// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.Parameter;
import java.util.Map;
import org.mariadb.r2dbc.codec.ParameterWithCodec;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.ServerPrepareResult;

public final class ExecutePacket implements ClientMessage {
  private final ParameterWithCodec[] parameters;
  private final int statementId;
  private final int parameterCount;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);

  public ExecutePacket(
      ServerPrepareResult prepareResult, Map<Integer, ParameterWithCodec> parameters) {
    // validate parameters
    if (prepareResult != null) {
      this.statementId = prepareResult.getStatementId();
      parameterCount = prepareResult.getNumParams();
    } else {
      this.statementId = -1;

      int parameterCount = 0;
      if (parameters != null) {
        Integer[] keys = parameters.keySet().toArray(new Integer[0]);
        for (Integer i : keys) {
          if (i + 1 > parameterCount) parameterCount = i + 1;
        }
      }
      this.parameterCount = parameterCount;
    }

    this.parameters = new ParameterWithCodec[parameterCount];

    for (int i = 0; i < this.parameterCount; i++) {
      ParameterWithCodec p = parameters.get(i);
      if (p == null) {
        throw new IllegalArgumentException(String.format("Parameter at position %s is not set", i));
      }
      this.parameters[i] = p;
    }
  }

  public MessageSequence getSequencer() {
    return sequencer;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer();
    buf.writeByte(0x17);
    buf.writeIntLE(statementId);
    buf.writeByte(0x00); // NO CURSOR
    buf.writeIntLE(1); // Iteration pos

    // create null bitmap
    if (parameterCount > 0) {
      int nullCount = (parameterCount + 7) / 8;

      byte[] nullBitsBuffer = new byte[nullCount];
      for (int i = 0; i < parameterCount; i++) {
        if (parameters[i].getValue() == null) {
          nullBitsBuffer[i / 8] |= (1 << (i % 8));
        }
      }
      buf.writeBytes(nullBitsBuffer);

      buf.writeByte(0x01); // Send Parameter type flag
      // Store types of parameters in first in first package that is sent to the server.
      for (int i = 0; i < parameterCount; i++) {
        buf.writeShortLE(parameters[i].getCodec().getBinaryEncodeType().get());
      }
    }

    // TODO avoid to send long data here.
    for (int i = 0; i < parameterCount; i++) {
      Parameter p = parameters[i];
      if (p.getValue() != null) {
        parameters[i].getCodec().encodeBinary(buf, context, p.getValue());
      }
    }
    return buf;
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.List;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.BindEncodedValue;
import org.mariadb.r2dbc.util.ServerPrepareResult;

public final class ExecutePacket implements ClientMessage {
  private final List<BindEncodedValue> bindValues;
  private final int statementId;
  private final int parameterCount;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);

  public ExecutePacket(ServerPrepareResult prepareResult, List<BindEncodedValue> bindValues) {
    this.bindValues = bindValues;
    this.statementId = prepareResult == null ? -1 : prepareResult.getStatementId();
    this.parameterCount = prepareResult == null ? bindValues.size() : prepareResult.getNumParams();
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
        if (bindValues.get(i).getValue() == null) {
          nullBitsBuffer[i / 8] |= (1 << (i % 8));
        }
      }
      buf.writeBytes(nullBitsBuffer);

      buf.writeByte(0x01); // Send Parameter type flag
      // Store types of parameters in first package that is sent to the server.
      for (int i = 0; i < parameterCount; i++) {
        buf.writeShortLE(bindValues.get(i).getCodec().getBinaryEncodeType().get());
      }
    }

    for (int i = 0; i < parameterCount; i++) {
      ByteBuf param = bindValues.get(i).getValue();
      if (param != null) {
        buf.writeBytes(param);
      }
    }

    return buf;
  }

  public MessageSequence getSequencer() {
    return sequencer;
  }

  @Override
  public void releaseEncodedBinds() {
    bindValues.forEach(
        b -> {
          if (b.getValue() != null) b.getValue().release();
        });
  }
}

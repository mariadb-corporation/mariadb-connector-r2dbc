// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Map;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.codec.Parameter;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class ExecutePacket implements ClientMessage {
  private final Map<Integer, Parameter<?>> parameters;
  private final int statementId;
  private final Sequencer sequencer = new Sequencer((byte) 0xff);

  public ExecutePacket(int statementId, Map<Integer, Parameter<?>> parameters) {
    this.parameters = parameters;
    this.statementId = statementId;
  }

  public Sequencer getSequencer() {
    return sequencer;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer();
    buf.writeByte(0x17);
    buf.writeIntLE(statementId);
    buf.writeByte(0x00); // NO CURSOR
    buf.writeIntLE(1); // Iteration pos

    Integer[] keys = parameters.keySet().toArray(new Integer[0]);
    int parameterCount = 0;
    for (Integer i : keys) {
      if (i + 1 > parameterCount) parameterCount = i + 1;
    }

    // create null bitmap
    if (parameterCount > 0) {
      int nullCount = (parameterCount + 7) / 8;

      byte[] nullBitsBuffer = new byte[nullCount];
      for (int i = 0; i < parameterCount; i++) {
        Parameter<?> p = parameters.get(i);
        if (p == null || p.isNull()) {
          nullBitsBuffer[i / 8] |= (1 << (i % 8));
        }
      }
      buf.writeBytes(nullBitsBuffer);

      buf.writeByte(0x01); // Send Parameter type flag
      // Store types of parameters in first in first package that is sent to the server.
      for (int i = 0; i < parameterCount; i++) {
        Parameter<?> p = parameters.get(i);
        if (p == null) {
          buf.writeShortLE(DataType.VARCHAR.get());
        } else {
          buf.writeShortLE(p.getBinaryEncodeType().get());
        }
      }
    }

    // TODO avoid to send long data here.
    for (int i = 0; i < parameterCount; i++) {
      Parameter<?> p = parameters.get(i);
      if (p != null && !p.isNull()) {
        p.encodeBinary(buf, context);
      }
    }
    return buf;
  }
}

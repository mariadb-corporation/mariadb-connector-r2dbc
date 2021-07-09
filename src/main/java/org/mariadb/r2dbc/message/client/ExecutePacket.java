// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.Parameter;
import java.util.Map;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.Codecs;
import org.mariadb.r2dbc.codec.list.StringCodec;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.ServerPrepareResult;

public final class ExecutePacket implements ClientMessage {
  private final Parameter[] parameters;
  private final Codec<?>[] codecs;
  private final int statementId;
  private final int parameterCount;
  private final Sequencer sequencer = new Sequencer((byte) 0xff);

  public ExecutePacket(ServerPrepareResult prepareResult, Map<Integer, Parameter> parameters) {
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

    this.codecs = new Codec<?>[parameterCount];
    this.parameters = new Parameter[parameterCount];

    for (int i = 0; i < this.parameterCount; i++) {

      Parameter p = parameters.get(i);
      if (p == null) {
        throw new IllegalArgumentException(String.format("Parameter at position %s is not set", i));
      }
      this.parameters[i] = p;
      if (p instanceof Parameter.In) {
        if (p.getValue() == null) {
          if (p.getType() != null) {
            try {
              codecs[i] = Codecs.codecByClass(p.getType().getJavaType(), i);
            } catch (IllegalArgumentException e) {
              codecs[i] = StringCodec.INSTANCE;
            }
          } else {
            codecs[i] = StringCodec.INSTANCE;
          }
        } else {
          codecs[i] = Codecs.codecByClass(p.getValue().getClass(), i);
        }
      } else {
        // out parameter
        if (p.getType() != null) {
          try {
            codecs[i] = Codecs.codecByClass(p.getType().getJavaType(), i);
          } catch (IllegalArgumentException e) {
            codecs[i] = StringCodec.INSTANCE;
          }
        } else {
          codecs[i] = StringCodec.INSTANCE;
        }
      }
    }
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
        buf.writeShortLE(codecs[i].getBinaryEncodeType().get());
      }
    }

    // TODO avoid to send long data here.
    for (int i = 0; i < parameterCount; i++) {
      Parameter p = parameters[i];
      if (p.getValue() != null) {
        codecs[i].encodeBinary(buf, context, p.getValue());
      }
    }
    return buf;
  }
}

/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Map;
import org.mariadb.r2dbc.client.Context;
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

    int parameterCount = parameters.size();
    // create null bitmap
    if (parameterCount > 0) {
      int nullCount = (parameterCount + 7) / 8;

      byte[] nullBitsBuffer = new byte[nullCount];
      for (int i = 0; i < parameterCount; i++) {
        if (parameters.get(i).isNull()) {
          nullBitsBuffer[i / 8] |= (1 << (i % 8));
        }
      }
      buf.writeBytes(nullBitsBuffer);

      buf.writeByte(0x01); // Send Parameter type flag
      // Store types of parameters in first in first package that is sent to the server.
      for (int i = 0; i < parameterCount; i++) {
        buf.writeShortLE(parameters.get(i).getBinaryEncodeType().get());
      }
    }

    // TODO avoid to send long data here.
    for (int i = 0; i < parameterCount; i++) {
      Parameter<?> parameter = parameters.get(i);
      if (!parameter.isNull()) {
        parameter.encodeBinary(buf, context);
      }
    }
    return buf;
  }

  @Override
  public String toString() {
    return "ExecutePacket{"
        + "statementId="
        + statementId
        + ", parameters="
        + parameters
        + ", sequencer="
        + sequencer
        + '}';
  }
}

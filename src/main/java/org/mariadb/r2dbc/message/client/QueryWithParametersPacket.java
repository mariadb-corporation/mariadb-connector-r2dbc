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
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Parameter;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.ClientPrepareResult;

public final class QueryWithParametersPacket implements ClientMessage {

  private final ClientPrepareResult prepareResult;
  private final Parameter<?>[] parameters;
  private final String[] generatedColumns;
  private final Sequencer sequencer = new Sequencer((byte) 0xff);

  public QueryWithParametersPacket(
      ClientPrepareResult prepareResult, Parameter<?>[] parameters, String[] generatedColumns) {
    this.prepareResult = prepareResult;
    this.parameters = parameters;
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
        parameters[i].encodeText(out, context);
        out.writeBytes(prepareResult.getQueryParts().get(i + 1));
      }
      if (additionalReturningPart != null)
        out.writeCharSequence(additionalReturningPart, StandardCharsets.UTF_8);
    }
    return out;
  }

  public Sequencer getSequencer() {
    return sequencer;
  }
}

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
import java.util.Objects;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.Assert;

public final class QueryPacket implements ClientMessage {

  private final String sql;
  private final Sequencer sequencer = new Sequencer((byte) 0xff);

  public QueryPacket(String sql) {
    this.sql = Assert.requireNonNull(sql, "query must not be null");
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator byteBufAllocator) {
    Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    ByteBuf out = byteBufAllocator.ioBuffer(this.sql.length() + 1);
    out.writeByte(0x03);
    out.writeCharSequence(this.sql, StandardCharsets.UTF_8);
    return out;
  }

  public Sequencer getSequencer() {
    return sequencer;
  }
}

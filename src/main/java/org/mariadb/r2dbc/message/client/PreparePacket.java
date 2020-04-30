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
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.Assert;

public final class PreparePacket implements ClientMessage {
  private final String sql;
  private final Sequencer sequencer = new Sequencer((byte) 0xff);

  public PreparePacket(String sql) {
    this.sql = Assert.requireNonNull(sql, "query must not be null");
  }

  public Sequencer getSequencer() {
    return sequencer;
  }

  @Override
  public ByteBuf encode(ConnectionContext context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer();
    buf.writeByte(0x16);
    buf.writeCharSequence(this.sql, StandardCharsets.UTF_8);
    return buf;
  }

  @Override
  public String toString() {
    return "PreparePacket{" + "sql='" + sql + '\'' + ", sequencer=" + sequencer + '}';
  }
}

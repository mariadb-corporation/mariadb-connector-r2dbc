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
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class AuthMoreRawPacket implements ClientMessage {

  private byte[] raw;
  private Sequencer sequencer;

  public AuthMoreRawPacket(Sequencer sequencer, byte[] raw) {
    this.sequencer = sequencer;
    this.raw = raw;
  }

  @Override
  public ByteBuf encode(ConnectionContext context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer(raw.length);
    buf.writeBytes(raw);
    return buf;
  }

  @Override
  public Sequencer getSequencer() {
    return sequencer;
  }

  @Override
  public String toString() {
    return "RawClientPacket{raw=******, sequencer=" + sequencer + '}';
  }
}

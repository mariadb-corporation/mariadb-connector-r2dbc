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
import org.mariadb.r2dbc.message.server.Sequencer;

public final class ClearPasswordPacket implements ClientMessage {

  private CharSequence password;
  private Sequencer sequencer;

  public ClearPasswordPacket(Sequencer sequencer, CharSequence password) {
    this.sequencer = sequencer;
    this.password = password;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    if (password == null) return allocator.ioBuffer(0);
    ByteBuf buf = allocator.ioBuffer(password.length() * 4);
    buf.writeCharSequence(password, StandardCharsets.UTF_8);
    buf.writeByte(0);
    return buf;
  }

  @Override
  public Sequencer getSequencer() {
    return sequencer;
  }
}

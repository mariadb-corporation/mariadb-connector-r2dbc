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
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class SslRequestPacket implements ClientMessage {

  private InitialHandshakePacket initialHandshakePacket;
  private long clientCapabilities;

  public SslRequestPacket(InitialHandshakePacket initialHandshakePacket, long clientCapabilities) {
    this.initialHandshakePacket = initialHandshakePacket;
    this.clientCapabilities = clientCapabilities;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {

    byte exchangeCharset =
        HandshakeResponse.decideLanguage(
            initialHandshakePacket.getDefaultCollation(),
            initialHandshakePacket.getMajorServerVersion(),
            initialHandshakePacket.getMinorServerVersion());

    ByteBuf buf = allocator.ioBuffer(32);

    buf.writeIntLE((int) clientCapabilities);
    buf.writeIntLE(1024 * 1024 * 1024);
    buf.writeByte(exchangeCharset); // 1 byte

    buf.writeZero(19); // 19  bytes
    buf.writeIntLE((int) (clientCapabilities >> 32)); // Maria extended flag
    return buf;
  }

  @Override
  public Sequencer getSequencer() {
    return initialHandshakePacket.getSequencer();
  }
}

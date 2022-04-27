// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;

public final class SslRequestPacket implements ClientMessage {

  private final InitialHandshakePacket initialHandshakePacket;
  private final long clientCapabilities;

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

    ByteBuf buf = allocator.buffer(32, 32);

    buf.writeIntLE((int) clientCapabilities);
    buf.writeIntLE(1024 * 1024 * 1024);
    buf.writeByte(exchangeCharset); // 1 byte

    buf.writeZero(19); // 19  bytes
    buf.writeIntLE((int) (clientCapabilities >> 32)); // Maria extended flag
    return buf;
  }

  @Override
  public MessageSequence getSequencer() {
    return initialHandshakePacket.getSequencer();
  }
}

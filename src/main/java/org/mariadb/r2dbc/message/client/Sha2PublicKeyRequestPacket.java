// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class Sha2PublicKeyRequestPacket implements ClientMessage {

  private Sequencer sequencer;

  public Sha2PublicKeyRequestPacket(Sequencer sequencer) {
    this.sequencer = sequencer;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer(1);
    buf.writeByte(0x02);
    return buf;
  }

  @Override
  public Sequencer getSequencer() {
    return sequencer;
  }
}

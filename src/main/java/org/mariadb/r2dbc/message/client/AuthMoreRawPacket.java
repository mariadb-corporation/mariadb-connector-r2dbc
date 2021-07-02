// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class AuthMoreRawPacket implements ClientMessage {

  private byte[] raw;
  private Sequencer sequencer;

  public AuthMoreRawPacket(Sequencer sequencer, byte[] raw) {
    this.sequencer = sequencer;
    this.raw = raw;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer(raw.length);
    buf.writeBytes(raw);
    return buf;
  }

  @Override
  public Sequencer getSequencer() {
    return sequencer;
  }
}

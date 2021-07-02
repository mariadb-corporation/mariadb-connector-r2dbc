// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.client.Context;

public class AuthMoreDataPacket implements ServerMessage {

  private Sequencer sequencer;
  private ByteBuf buf;

  private AuthMoreDataPacket(Sequencer sequencer, ByteBuf buf) {
    this.sequencer = sequencer;
    this.buf = buf;
  }

  public static AuthMoreDataPacket decode(Sequencer sequencer, ByteBuf buf, Context context) {
    buf.skipBytes(1);
    ByteBuf data = buf.readRetainedSlice(buf.readableBytes());
    return new AuthMoreDataPacket(sequencer, data);
  }

  public void deallocate() {
    if (buf != null) {
      buf.release();
      buf = null;
    }
  }

  public Sequencer getSequencer() {
    return sequencer;
  }

  public ByteBuf getBuf() {
    return buf;
  }

  @Override
  public boolean ending() {
    return true;
  }
}

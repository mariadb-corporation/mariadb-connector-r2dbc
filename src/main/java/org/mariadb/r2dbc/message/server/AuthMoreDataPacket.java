// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.ServerMessage;

public class AuthMoreDataPacket implements AuthMoreData, ServerMessage {

  private MessageSequence sequencer;
  private ByteBuf buf;

  private AuthMoreDataPacket(MessageSequence sequencer, ByteBuf buf) {
    this.sequencer = sequencer;
    this.buf = buf;
  }

  public static AuthMoreDataPacket decode(MessageSequence sequencer, ByteBuf buf, Context context) {
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

  public MessageSequence getSequencer() {
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

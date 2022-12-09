// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.ServerMessage;

public class AuthMoreDataPacket implements AuthMoreData, ServerMessage, ReferenceCounted {

  private final MessageSequence sequencer;
  private final ByteBuf buf;

  private AuthMoreDataPacket(MessageSequence sequencer, ByteBuf buf) {
    this.sequencer = sequencer;
    this.buf = buf;
  }

  public static AuthMoreDataPacket decode(MessageSequence sequencer, ByteBuf buf, Context context) {
    buf.skipBytes(1);
    ByteBuf data = buf.readRetainedSlice(buf.readableBytes());
    return new AuthMoreDataPacket(sequencer, data);
  }

  @Override
  public int refCnt() {
    return buf.refCnt();
  }

  @Override
  public ReferenceCounted retain() {
    buf.retain();
    return this;
  }

  @Override
  public ReferenceCounted retain(int increment) {
    buf.retain(increment);
    return this;
  }

  @Override
  public ReferenceCounted touch() {
    buf.touch();
    return this;
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    buf.touch(hint);
    return this;
  }

  public boolean release() {
    return buf.release();
  }

  @Override
  public boolean release(int decrement) {
    return buf.release();
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

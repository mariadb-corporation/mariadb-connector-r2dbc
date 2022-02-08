// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.message.AuthSwitch;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;

public class AuthSwitchPacket implements AuthSwitch, ServerMessage {

  private Sequencer sequencer;
  private String plugin;
  private byte[] seed;

  public AuthSwitchPacket(Sequencer sequencer, String plugin, byte[] seed) {
    this.sequencer = sequencer;
    this.plugin = plugin;
    this.seed = seed;
  }

  public static AuthSwitchPacket decode(Sequencer sequencer, ByteBuf buf, Context context) {
    buf.skipBytes(1);
    int nullLength = buf.bytesBefore((byte) 0x00);
    String plugin = buf.toString(buf.readerIndex(), nullLength, StandardCharsets.US_ASCII);
    buf.skipBytes(nullLength + 1);

    byte[] seed = new byte[buf.readableBytes()];
    buf.getBytes(buf.readerIndex(), seed);
    return new AuthSwitchPacket(sequencer, plugin, seed);
  }

  public String getPlugin() {
    return plugin;
  }

  public byte[] getSeed() {
    return seed;
  }

  public Sequencer getSequencer() {
    return sequencer;
  }

  @Override
  public boolean ending() {
    return true;
  }
}

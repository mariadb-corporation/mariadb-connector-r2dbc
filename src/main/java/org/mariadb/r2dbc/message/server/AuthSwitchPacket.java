// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.mariadb.r2dbc.message.AuthSwitch;
import org.mariadb.r2dbc.message.ServerMessage;

public class AuthSwitchPacket implements AuthSwitch, ServerMessage {

  private final Sequencer sequencer;
  private final String plugin;
  private final byte[] seed;

  public AuthSwitchPacket(Sequencer sequencer, String plugin, byte[] seed) {
    this.sequencer = sequencer;
    this.plugin = plugin;
    this.seed = seed;
  }

  public static AuthSwitchPacket decode(Sequencer sequencer, ByteBuf buf) {
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

  /**
   * Return the seed without its trailing 0x00 byte. A malicious/buggy server may send an empty
   * seed, so guard the length to avoid {@code new byte[-1]} (NegativeArraySizeException).
   *
   * @param seed seed sent by the server
   * @return seed without the trailing null byte (empty if the seed is empty)
   */
  public static byte[] getTruncatedSeed(byte[] seed) {
    return (seed.length > 0) ? Arrays.copyOfRange(seed, 0, seed.length - 1) : new byte[0];
  }

  public Sequencer getSequencer() {
    return sequencer;
  }

  @Override
  public boolean ending() {
    return true;
  }
}

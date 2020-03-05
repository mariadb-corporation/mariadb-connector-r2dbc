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

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.mariadb.r2dbc.client.ConnectionContext;

public class AuthSwitchPacket implements ServerMessage {

  private Sequencer sequencer;
  private String plugin;
  private byte[] seed;

  public AuthSwitchPacket(Sequencer sequencer, String plugin, byte[] seed) {
    this.sequencer = sequencer;
    this.plugin = plugin;
    this.seed = seed;
  }

  public static AuthSwitchPacket decode(
      Sequencer sequencer, ByteBuf buf, ConnectionContext context) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AuthSwitchPacket that = (AuthSwitchPacket) o;
    return Objects.equals(sequencer, that.sequencer)
        && Objects.equals(plugin, that.plugin)
        && Arrays.equals(seed, that.seed);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(sequencer, plugin);
    result = 31 * result + Arrays.hashCode(seed);
    return result;
  }

  @Override
  public String toString() {
    return "AuthSwitchPacket{"
        + "sequencer="
        + sequencer
        + ", plugin='"
        + plugin
        + '\''
        + ", seed="
        + Arrays.toString(seed)
        + '}';
  }
}

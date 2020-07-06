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
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import org.mariadb.r2dbc.client.Context;

public class AuthMoreDataPacket extends AbstractReferenceCounted implements ServerMessage {

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

  @Override
  public void deallocate() {
    if (buf != null) {
      buf.release();
      buf = null;
    }
  }

  public Sequencer getSequencer() {
    return sequencer;
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    return this;
  }

  public ByteBuf getBuf() {
    return buf;
  }

  @Override
  public boolean ending() {
    return true;
  }

  @Override
  public String toString() {
    return "AuthMoreDataPacket{" + "sequencer=" + sequencer + ", buf=" + buf + '}';
  }
}
